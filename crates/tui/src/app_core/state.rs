// ── TuiState — Unified TUI application state ──────────────────
// Composes the domain App with terminal engine components:
//   LayoutTree, KeybindEngine, EventBus, ThemeEngine, DialogManager.
//
// Bridges App::apply_event(CowdEvent) → EventBus for new components.
// Orchestrates rendering via direct component layout + ChatView + dialogs.
//
// Architecture:
//   - TuiState OWNS App (not a reference)
//   - TuiState::apply_event() adds EventBus bridging around App::apply_event()
//   - handle_input() → KeybindEngine → Action dispatch
//   - render() → sync ChatView → render_tree → render dialogs
// -------------------------------------------------------------------

#![allow(dead_code)]

#[path = "state/overlay.rs"]
mod overlay;
#[path = "state/render.rs"]
mod render;
#[path = "state/session.rs"]
mod session;
#[path = "state/shell.rs"]
mod shell;
#[path = "state/workbench.rs"]
mod workbench;

pub use overlay::OverlayState;
pub use session::SessionUiState;
pub use shell::ShellState;
pub use workbench::WorkbenchState;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
static SESSION_CATALOG_MATERIALIZATIONS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn reset_session_catalog_materializations() {
    SESSION_CATALOG_MATERIALIZATIONS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
fn session_catalog_materializations() -> usize {
    SESSION_CATALOG_MATERIALIZATIONS.load(Ordering::Relaxed)
}

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;

use crate::accessibility::AccessibilityMode;
use crate::animation::{AnimationEngine, AnimationKind};
use crate::app::{App, SkillSummary, SystemNoticeKind};
use crate::app_surface_host::{AppSurfaceCommand, DeclarativeAppHost};
use crate::components::activity_panel::ActivityPanel;
use crate::components::agent_team_panel::AgentTeamPanel;
use crate::components::agents_overlay::AgentsOverlay;
use crate::components::approval_cockpit_panel::ApprovalCockpitPanel;
use crate::components::chat_view::ChatView;
use crate::components::command_palette::CommandPalette;
use crate::components::composer::Composer;
use crate::components::config_panel::ConfigPanel;
use crate::components::context_panel::ContextPanel;
use crate::components::context_suggestions::ContextSuggestions;
use crate::components::dialog::DialogManager;
use crate::components::diff_viewer::DiffViewer;
use crate::components::export_dialog::ExportDialog;
use crate::components::file_changes_panel::FileChangesPanel;
use crate::components::file_tree::FileTree;
use crate::components::gateway_panel::GatewayPanel;
use crate::components::goal_workbench_panel::GoalWorkbenchPanel;
use crate::components::memory_panel::MemoryPanel;
use crate::components::performance_dashboard::PerformanceDashboard;
use crate::components::prompt::Prompt;
use crate::components::reality_panel::RealityPanel;
use crate::components::revert_dialog::RevertDialog;
use crate::components::runtime_activity_panel::RuntimeActivityPanel;
use crate::components::session_sidebar::SessionSidebar;
use crate::components::skills_panel::SkillsPanel;
use crate::components::status_bar::StatusBar;
use crate::components::surface_panel::SurfacePanel;
use crate::components::system_status_bar::SystemStatusBar;
use crate::components::thinking_panel::ThinkingPanel;
use crate::components::toast::{ToastManager, ToastVariant};
use crate::components::todo_panel::TodoPanel;
use crate::components::tool_ops_panel::{ToolOpsMode, ToolOpsPanel};
use crate::components::{Component, RenderContext};
use crate::context_tokens::{validate_context_tokens_against_entries, ContextWorkspaceEntry};
use crate::error_recovery::{self, RenderResult};
use crate::event::dispatcher::EventDispatcher;
use crate::event::{ComponentId as EventComponentId, EventBus};
use crate::keybind::types::Action;
use crate::keybind::which_key::WhichKey;
use crate::keybind::{default_bindings, KeybindEngine};
use crate::layout::LayoutState;
use crate::profiler::{FrameTimer, RenderProfiler};
use crate::theme::ThemeEngine;
use crate::workbench::panel_registry;
use crate::CowdEvent;

/// Result of processing a key event through the TUI input pipeline.
#[derive(Debug, Clone)]
pub enum ProcessedKey {
    Submit(String),
    Cancel,
    Exit,
    Nothing,
}

pub(crate) const SIDEBAR_TAB_COUNT: usize = 11;
pub(crate) const TAB_RUNTIME: usize = 0;
pub(crate) const TAB_TOOLS: usize = 1;
pub(crate) const TAB_CHANGES: usize = 2;
pub(crate) const TAB_GOALS: usize = 3;
pub(crate) const TAB_APPROVALS: usize = 4;
pub(crate) const TAB_TODO: usize = 5;
pub(crate) const TAB_FILES: usize = 6;
pub(crate) const TAB_SESSIONS: usize = 7;
pub(crate) const TAB_SURFACES: usize = 8;
pub(crate) const TAB_APPS: usize = 9;
pub(crate) const TAB_GATEWAY: usize = 10;

/// A declarative APP command waiting for the Gateway-owned client.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingAppSurfaceCommand {
    pub session_id: String,
    pub authority_generation: u64,
    pub command: AppSurfaceCommand,
}

pub(crate) use crate::effect::{CompletedCoreGatewayEffect, PendingCoreGatewayEffect};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FocusTarget {
    Chat,
    Input,
    Activity,
    Sidebar,
    TopicPanel(SidebarTopicPanel),
    CommandPalette,
    PromptSuggestions,
    Dialog,
}

impl FocusTarget {
    fn label(self) -> &'static str {
        match self {
            FocusTarget::Chat => "chat",
            FocusTarget::Input => "input",
            FocusTarget::Activity => "activity",
            FocusTarget::Sidebar => "sidebar",
            FocusTarget::TopicPanel(SidebarTopicPanel::Diff) => "diff",
            FocusTarget::TopicPanel(SidebarTopicPanel::Memory) => "memory",
            FocusTarget::TopicPanel(SidebarTopicPanel::Skills) => "skills",
            FocusTarget::TopicPanel(SidebarTopicPanel::Config) => "config",
            FocusTarget::TopicPanel(SidebarTopicPanel::Reality) => "reality",
            FocusTarget::CommandPalette => "palette",
            FocusTarget::PromptSuggestions => "suggest",
            FocusTarget::Dialog => "dialog",
        }
    }

    fn hint(self) -> &'static str {
        match self {
            FocusTarget::Chat => "j/k scroll · / commands · Ctrl+P palette · Ctrl+B panels",
            FocusTarget::Input => {
                "Enter send · Alt+Enter/Ctrl+J newline · Ctrl+F search · / commands · Esc clear"
            }
            FocusTarget::Activity => "j/k scroll · PgUp/PgDn page · Esc close",
            FocusTarget::Sidebar => "Tab switch · j/k scroll · Esc close · /focus input",
            FocusTarget::TopicPanel(SidebarTopicPanel::Diff) => {
                "j/k scroll · n next hunk · m reviewed · Esc close"
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Memory) => {
                "j/k select · Enter detail · / search · Esc back/close"
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Skills) => {
                "j/k select · Tab category · Enter detail · Esc close"
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Config) => {
                "j/k select model · Enter set · r reload · e refresh · Esc close"
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Reality) => {
                "1 overview · 2 samples · j/k scroll · Esc close"
            }
            FocusTarget::CommandPalette => "type to filter · j/k move · Enter run · Esc close",
            FocusTarget::PromptSuggestions => "Tab accept · arrows move · Esc close",
            FocusTarget::Dialog => "Tab move · Enter confirm · Esc close",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SidebarTopicPanel {
    Diff,
    Memory,
    Skills,
    Config,
    Reality,
}

impl SidebarTopicPanel {
    fn label(self) -> &'static str {
        match self {
            SidebarTopicPanel::Diff => "Diff",
            SidebarTopicPanel::Memory => "Memory",
            SidebarTopicPanel::Skills => "Skills",
            SidebarTopicPanel::Config => "Config",
            SidebarTopicPanel::Reality => "Reality",
        }
    }
}

fn sidebar_tab_labels(width: u16) -> Vec<&'static str> {
    panel_registry::sidebar_labels(width)
}

fn char_col_to_byte_offset(text: &str, col: usize) -> usize {
    text.char_indices()
        .nth(col)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len())
}

#[derive(Debug, Clone, Copy, Default)]
struct TuiHitAreas {
    chat: ratatui::layout::Rect,
    activity: Option<ratatui::layout::Rect>,
    sidebar: Option<ratatui::layout::Rect>,
    topic: Option<ratatui::layout::Rect>,
    input: ratatui::layout::Rect,
}

impl TuiHitAreas {
    fn contains(area: ratatui::layout::Rect, x: u16, y: u16) -> bool {
        x >= area.x
            && x < area.x.saturating_add(area.width)
            && y >= area.y
            && y < area.y.saturating_add(area.height)
    }
}

// ── TuiState ────────────────────────────────────────────────────

/// Explicit composition root for the terminal read model and UI reducers.
pub struct TuiState {
    pub app: App,
    pub shell: ShellState,
    pub session: SessionUiState,
    pub workbench: WorkbenchState,
    pub overlay: OverlayState,
}

impl TuiState {
    // ── Construction ────────────────────────────────────────────

    /// Create a fully-initialized `TuiState` with all engines set to
    /// sensible defaults.
    ///
    /// - `App` is constructed with the given model and session ID.
    /// - `LayoutTree` has a simple empty split (placeholder for future panels).
    /// - `ChatView` is freshly created (empty timeline).
    /// - `KeybindEngine` uses `default_bindings()` (vim/emacs-style chords).
    /// - `EventBus` is empty, ready for component injection.
    /// - `EventDispatcher` is empty, components registered via `register()`.
    /// - `ThemeEngine` is pre-loaded with the builtin dark theme.
    /// - `DialogManager` is empty (no dialogs shown).
    #[must_use]
    pub fn new(model: &str, session_id: &str) -> Self {
        let app = App::new(model, session_id);

        // Layout tree starts with the sidebar hidden. Ctrl+B or a panel-focused
        // action opens it on demand.
        let mut layout_tree = crate::layout::defaults::build_default_layout();
        let mut layout_state = LayoutState::default();
        layout_state.toggle_sidebar(&mut layout_tree);

        let chat_view = ChatView::new();
        let keybind_engine = KeybindEngine::new(default_bindings());
        let event_bus = EventBus::new();
        let event_dispatcher = EventDispatcher::new();
        let theme_engine = ThemeEngine::new_dark();
        let dialog_manager = DialogManager::new();
        let toast_manager = ToastManager::new();
        let agents_overlay = AgentsOverlay::new();
        let agent_team_panel = AgentTeamPanel::new();
        let l4_memory_view = L4MemoryView::new();
        let thinking_panel = ThinkingPanel::new();
        let command_palette = CommandPalette::new();
        let question_form = None;
        let export_dialog = ExportDialog::new();
        let export_dialog_active = false;
        let pending_export_options = None;
        let revert_dialog = RevertDialog::new();
        let context_panel = ContextPanel::new();
        let context_suggestions = ContextSuggestions::new();
        let file_changes_panel = FileChangesPanel::new();
        let todo_panel = TodoPanel::new();
        let goal_workbench_panel = GoalWorkbenchPanel::new();
        let approval_cockpit_panel = ApprovalCockpitPanel::new();
        let status_bar = StatusBar::with_default_sections();
        let animation_engine = AnimationEngine::new();
        let frame_timer = FrameTimer::new();
        let render_profiler = RenderProfiler::new();
        let accessibility = AccessibilityMode::new();

        let diff_viewer = DiffViewer::new("Diff");
        let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let prompt = Prompt::new(cwd);
        let composer = Composer::new();
        let file_tree = FileTree::new();
        let session_sidebar = SessionSidebar::new(session_id);
        let memory_panel = MemoryPanel::new();
        let reality_panel = RealityPanel::new();
        let performance_dashboard = PerformanceDashboard::new();
        let skills_panel = SkillsPanel::new();
        let config_panel = ConfigPanel::new();
        let gateway_panel = GatewayPanel::new();
        let surface_panel = SurfacePanel::new();
        let app_surface_host = DeclarativeAppHost::empty();
        let runtime_activity_panel = RuntimeActivityPanel::new();
        let tool_ops_panel = ToolOpsPanel::new();
        let system_status_bar = SystemStatusBar::new();
        let activity_panel = ActivityPanel::new();

        let mut state = Self {
            app,
            shell: ShellState {
                layout_tree,
                layout_state,
                chat_view,
                keybind_engine,
                event_bus,
                event_dispatcher,
                theme_engine,
                status_bar,
                animation_engine,
                frame_timer,
                render_profiler,
                prompt,
                composer,
                composer_content_width: 78,
                composer_desired_column: None,
                focus_target: FocusTarget::Chat,
                last_hit_areas: TuiHitAreas::default(),
                accessibility,
                active_sessions: 0,
                startup_phase: StartupPhase::Hidden,
                startup_start: Instant::now(),
                startup_show_time: None,
                dropped_events: 0,
                pending_cancel: false,
                pending_quit: false,
                last_terminal_width: 80,
            },
            session: SessionUiState {
                memory_projection_available: false,
                memory_panel_last_sync: None,
                session_sidebar,
                app_surface_host,
                pending_app_surface_commands: Vec::new(),
                pending_core_gateway_effects: Vec::new(),
                authority_generation: 1,
                authorization_revoked: false,
                session_catalog_fingerprint: None,
            },
            workbench: WorkbenchState {
                agent_team_panel,
                l4_memory_view,
                context_panel,
                file_changes_panel,
                todo_panel,
                goal_workbench_panel,
                approval_cockpit_panel,
                file_tree,
                memory_panel,
                reality_panel,
                skills_panel,
                config_panel,
                gateway_panel,
                surface_panel,
                runtime_activity_panel,
                tool_ops_panel,
                system_status_bar,
                activity_panel,
                activity_panel_visible: false,
                sidebar_active_tab: 0,
                active_topic_panel: None,
            },
            overlay: OverlayState {
                dialog_manager,
                pending_approval_dialog: None,
                toast_manager,
                agents_overlay,
                thinking_panel,
                command_palette,
                question_form,
                export_dialog,
                export_dialog_active,
                pending_export_options,
                revert_dialog,
                context_suggestions,
                diff_viewer,
                performance_dashboard,
            },
        };
        state.flush_app_surface_commands();
        state.sync_app_palette_actions();
        state
    }

    /// Build a `TuiState` from an existing `App`, preserving all app state.
    /// The app is moved in; call `into_app()` to extract it back after rendering.
    #[must_use]
    pub fn from_app(app: App) -> Self {
        let mut state = Self::new(&app.shell.model, &app.shell.session_id);
        state.app = app;
        state
    }

    /// Extract the inner `App`, consuming this `TuiState`.
    #[must_use]
    pub fn into_app(self) -> App {
        self.app
    }

    // ── Event Bridging ──────────────────────────────────────────

    /// Apply a `CowdEvent` from the background turn runner to the display.
    ///
    /// **Preserves existing behavior**: delegates to `App::apply_event()`
    /// for all timeline updates, token tracking, and state transitions.
    ///
    /// **Bridges to new EventBus**: after updating the App, emits a typed
    /// internal state-change notification. It is intentionally not encoded
    /// as a fake terminal event.
    pub fn apply_event(&mut self, event: CowdEvent) {
        let apply_started = std::time::Instant::now();
        if let CowdEvent::AppSurface { event } = event {
            self.session.app_surface_host.apply_event(event);
            self.flush_app_surface_commands();
            self.sync_app_palette_actions();
            self.shell.event_bus.notify_state_changed();
            self.shell.event_dispatcher.dispatch(&self.shell.event_bus);
            crate::performance::observe_duration("tui_event_apply_ms", apply_started.elapsed());
            return;
        }

        match &event {
            CowdEvent::TurnStarted => self.app.execution.turn_interaction.submit_started(),
            CowdEvent::GatewaySession {
                event:
                    crate::protocol::GatewaySessionEvent::UserMessageCommitted { correlation, .. },
            } => {
                if let Some(execution_id) = correlation.execution_id.as_deref() {
                    let selects_visible_execution =
                        self.app.execution.current_execution_id.is_none()
                            || self.app.execution.current_execution_id.as_deref()
                                == Some(execution_id)
                            || !self.app.turn_is_active()
                            || self.app.execution.current_execution_status.is_some_and(
                                harness_contract::projection::ExecutionLiveStatus::is_terminal,
                            );
                    if selects_visible_execution {
                        self.app
                            .execution
                            .turn_interaction
                            .ingress_accepted(execution_id);
                    }
                }
            }
            CowdEvent::GatewaySession {
                event:
                    crate::protocol::GatewaySessionEvent::ExecutionPhase {
                        correlation,
                        status,
                        ..
                    },
            } if *status != harness_contract::projection::ExecutionLiveStatus::Queued
                && !correlation
                    .execution_id
                    .as_deref()
                    .is_some_and(|execution_id| {
                        self.app.execution_is_terminalized(execution_id)
                    }) =>
            {
                if let Some(execution_id) = correlation.execution_id.as_deref() {
                    self.app
                        .execution
                        .turn_interaction
                        .ingress_accepted(execution_id);
                }
            }
            CowdEvent::ExecutionGraphSummary { summary } => {
                if let Some(execution_id) = summary.graph_id.as_deref() {
                    self.app
                        .execution
                        .turn_interaction
                        .ingress_accepted(execution_id);
                }
            }
            CowdEvent::TurnError { .. } => {}
            CowdEvent::SessionInputProjection { .. } => {}
            CowdEvent::SessionInputDispositionChanged { .. } => {}
            CowdEvent::Warning { message } if message.contains("projection stream interrupted") => {
                self.app.execution.turn_interaction.reconnecting();
            }
            _ => {}
        }
        if let CowdEvent::TurnError { ref error } = event {
            self.overlay.toast_manager.push(
                ToastVariant::Error,
                Some("Error".into()),
                error.clone(),
                5000,
            );
        }
        self.app.apply_event(event);
        self.shell.event_bus.notify_state_changed();
        self.shell.event_dispatcher.dispatch(&self.shell.event_bus);
        crate::performance::observe_duration("tui_event_apply_ms", apply_started.elapsed());
    }

    fn flush_app_surface_commands(&mut self) {
        for message in self.session.app_surface_host.take_notices() {
            self.overlay.toast_manager.push(
                ToastVariant::Warning,
                Some("Applications".into()),
                message,
                5000,
            );
        }
        self.session.pending_app_surface_commands.extend(
            self.session
                .app_surface_host
                .take_commands()
                .into_iter()
                .map(|command| PendingAppSurfaceCommand {
                    session_id: self.app.shell.session_id.clone(),
                    authority_generation: self.session.authority_generation,
                    command,
                }),
        );
    }

    fn apply_app_navigation_effect(&mut self, route: &str, context: Option<&serde_json::Value>) {
        let is_backlink_completion = context.is_some_and(|context| {
            context.get("kind").and_then(serde_json::Value::as_str) == Some("backlink")
                && (context
                    .get("object")
                    .is_some_and(|object| !object.is_null())
                    || context
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|message| !message.trim().is_empty()))
        });
        // Initial navigation opens and focuses the destination, then records
        // the pending canonical target. Its asynchronous completion must not
        // reopen the sidebar: reopening clears the pending identity and also
        // steals focus if the operator has moved elsewhere.
        if !is_backlink_completion {
            self.open_surface_for_slash_result(route.trim_start_matches('/'));
        }
        if let Some(context) = context {
            self.apply_app_navigation_context(context);
        }
    }

    /// Apply a host-neutral application backlink after opening its core
    /// destination. The linked APP chooses the route and carries an opaque
    /// context envelope; Cowd accepts a resolved object only after validating
    /// the canonical resource identity for the destination panel.
    fn apply_app_navigation_context(&mut self, context: &serde_json::Value) {
        if context.get("kind").and_then(serde_json::Value::as_str) != Some("backlink") {
            return;
        }
        let Some(target) = context
            .get("target")
            .and_then(serde_json::Value::as_str)
            .filter(|target| !target.trim().is_empty())
        else {
            return;
        };
        let object = context.get("object").filter(|object| !object.is_null());
        let failure = context
            .get("error")
            .and_then(serde_json::Value::as_str)
            .filter(|message| !message.trim().is_empty());

        if target.starts_with("runtime-execution://")
            || target.starts_with("application-execution://")
            || target.starts_with("task://")
        {
            if object.is_none() && failure.is_none() {
                self.workbench
                    .runtime_activity_panel
                    .focus_backlink_target(target);
                return;
            }
            if !self
                .workbench
                .runtime_activity_panel
                .accepts_backlink_result(target)
            {
                return;
            }
            if let Some(object) = object {
                if runtime_backlink_object_matches_target(target, object) {
                    if target.starts_with("runtime-execution://") {
                        if let Ok(projection) = serde_json::from_value::<
                            crate::protocol::ExecutionProjection,
                        >(object.clone())
                        {
                            if crate::protocol::validate_execution_projection_schema(&projection)
                                .is_ok()
                            {
                                self.app.apply_execution_projection(projection);
                            }
                        }
                    }
                    self.workbench
                        .runtime_activity_panel
                        .record_backlink_object(target, object);
                } else {
                    self.workbench.runtime_activity_panel.record_backlink_failure(
                        target,
                        "Application returned an object whose canonical identity does not match the backlink",
                    );
                }
            } else if let Some(message) = failure {
                self.workbench
                    .runtime_activity_panel
                    .record_backlink_failure(target, message);
            }
            return;
        }

        if target.starts_with("evidence://") {
            if object.is_none() && failure.is_none() {
                self.workbench.reality_panel.focus_backlink_target(target);
                return;
            }
            if !self.workbench.reality_panel.accepts_backlink_result(target) {
                return;
            }
            if let Some(object) = object {
                if evidence_backlink_object_matches_target(target, object) {
                    self.workbench
                        .reality_panel
                        .record_backlink_object(target, object.clone());
                } else {
                    self.workbench.reality_panel.record_backlink_failure(
                        target,
                        "Application returned evidence whose canonical identity does not match the backlink",
                    );
                }
            } else if let Some(message) = failure {
                self.workbench
                    .reality_panel
                    .record_backlink_failure(target, message);
            }
            return;
        }

        if target.starts_with("approval://") {
            if object.is_none() && failure.is_none() {
                self.workbench
                    .approval_cockpit_panel
                    .focus_backlink_target(target);
                return;
            }
            if !self
                .workbench
                .approval_cockpit_panel
                .accepts_backlink_result(target)
            {
                return;
            }
            if let Some(object) = object {
                if approval_backlink_object_matches_target(target, object) {
                    self.workbench
                        .approval_cockpit_panel
                        .record_backlink_object(target, object);
                } else {
                    self.workbench.approval_cockpit_panel.record_backlink_failure(
                        target,
                        "Application returned an approval whose canonical identity does not match the backlink",
                    );
                }
            } else if let Some(message) = failure {
                self.workbench
                    .approval_cockpit_panel
                    .record_backlink_failure(target, message);
            }
            return;
        }

        if target.starts_with("receipt://cross-plane/") || target.starts_with("surface://") {
            if object.is_none() && failure.is_none() {
                self.workbench.surface_panel.focus_backlink_target(target);
                return;
            }
            if !self.workbench.surface_panel.accepts_backlink_result(target) {
                return;
            }
            if let Some(object) = object {
                if surface_backlink_receipt_matches_target(target, object) {
                    self.workbench
                        .surface_panel
                        .record_backlink_receipt(target, object.clone());
                } else {
                    self.workbench.surface_panel.record_backlink_failure(
                        target,
                        "Application returned a Surface receipt whose canonical identity does not match the backlink",
                    );
                }
            } else if let Some(message) = failure {
                self.workbench
                    .surface_panel
                    .record_backlink_failure(target, message);
            }
        }
    }

    pub(crate) fn take_pending_app_surface_commands(&mut self) -> Vec<PendingAppSurfaceCommand> {
        self.flush_app_surface_commands();
        std::mem::take(&mut self.session.pending_app_surface_commands)
    }

    pub(crate) fn queue_gateway_api<F, Fut, C>(&mut self, operation: F, completion: C)
    where
        F: FnOnce(crate::gateway_client::GatewayApiClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<serde_json::Value, crate::gateway_client::GatewayApiError>>
            + Send
            + 'static,
        C: FnOnce(&mut TuiState, Result<serde_json::Value, String>) + Send + 'static,
    {
        self.session
            .pending_core_gateway_effects
            .push(PendingCoreGatewayEffect {
                session_id: self.app.shell.session_id.clone(),
                authority_generation: self.session.authority_generation,
                operation: Box::new(move |client| Box::pin(operation(client))),
                completion: Box::new(completion),
            });
        self.app.request_redraw();
    }

    pub(crate) fn authority_generation(&self) -> u64 {
        crate::selectors::authority_generation(self)
    }

    pub(crate) fn accepts_authority(&self, session_id: &str, generation: u64) -> bool {
        crate::selectors::accepts_authority(self, session_id, generation)
    }

    pub(crate) fn revoke_session_authority(&mut self, reason: &str) {
        crate::reducer::reduce(
            self,
            crate::action::UiAction::RevokeSessionAuthority {
                reason: reason.to_owned(),
            },
        );
    }

    pub(crate) fn install_session_authority(&mut self, generation: u64) {
        crate::reducer::reduce(
            self,
            crate::action::UiAction::InstallSessionAuthority { generation },
        );
    }

    pub(crate) fn take_pending_core_gateway_effects(&mut self) -> Vec<PendingCoreGatewayEffect> {
        std::mem::take(&mut self.session.pending_core_gateway_effects)
    }

    pub(crate) fn apply_gateway_session_catalog(&mut self, payload: &serde_json::Value) -> bool {
        use std::hash::{Hash, Hasher};

        let raw_sessions = payload
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        raw_sessions.len().hash(&mut hasher);
        for session in raw_sessions {
            session
                .get("id")
                .and_then(serde_json::Value::as_str)
                .hash(&mut hasher);
            session
                .get("title")
                .and_then(serde_json::Value::as_str)
                .hash(&mut hasher);
            session
                .get("status")
                .and_then(serde_json::Value::as_str)
                .hash(&mut hasher);
            session
                .get("updated_at")
                .and_then(serde_json::Value::as_str)
                .hash(&mut hasher);
            session
                .get("message_count")
                .and_then(serde_json::Value::as_u64)
                .hash(&mut hasher);
        }
        let fingerprint = hasher.finish();
        if self.session.session_catalog_fingerprint == Some(fingerprint) {
            return false;
        }
        let sessions = raw_sessions
            .iter()
            .filter_map(|session| {
                let id = session
                    .get("id")
                    .and_then(serde_json::Value::as_str)?
                    .to_string();
                #[cfg(test)]
                SESSION_CATALOG_MATERIALIZATIONS.fetch_add(1, Ordering::Relaxed);
                let updated_at_ms = session
                    .get("updated_at")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
                    .map(|timestamp| timestamp.timestamp_millis().max(0) as u64)
                    .unwrap_or_default();
                Some(crate::app::SessionSummary {
                    id,
                    title: session
                        .get("title")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    path: session
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                        .to_string(),
                    updated_at_ms,
                    message_count: session
                        .get("message_count")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default() as usize,
                })
            })
            .collect::<Vec<_>>();
        crate::reducer::reduce(
            self,
            crate::action::UiAction::ApplySessionCatalog {
                fingerprint,
                sessions,
            },
        )
    }

    pub(crate) fn set_gateway_app_catalog(
        &mut self,
        catalog: cowd_app_protocol::AppCatalogV1,
    ) -> Result<(), String> {
        self.session.pending_app_surface_commands.clear();
        self.session.app_surface_host.install_catalog(catalog)?;
        self.flush_app_surface_commands();
        self.sync_app_palette_actions();
        Ok(())
    }

    pub(crate) fn gateway_app_catalog_entry(
        &self,
        app_id: &str,
    ) -> Option<cowd_app_protocol::AppCatalogEntryV1> {
        self.session.app_surface_host.catalog_entry(app_id)
    }

    pub(crate) fn reject_gateway_app_detail(&mut self, app_id: &str, error: String) {
        self.session.app_surface_host.reject_contract(app_id, error);
        self.flush_app_surface_commands();
        self.sync_app_palette_actions();
    }

    fn sync_app_palette_actions(&mut self) {
        let actions = self.session.app_surface_host.actions();
        self.overlay.command_palette.sync_app_actions(&actions);
    }

    fn cycle_app_panel(&mut self, reverse: bool) -> bool {
        let cycled = self.session.app_surface_host.cycle_app(reverse);
        self.flush_app_surface_commands();
        self.sync_app_palette_actions();
        cycled
    }

    fn handle_app_panel_key(&mut self, key: KeyEvent) -> bool {
        if !self.shell.layout_state.sidebar_visible
            || self.workbench.active_topic_panel.is_some()
            || self.workbench.sidebar_active_tab != TAB_APPS
            || self.shell.focus_target != FocusTarget::Sidebar
        {
            return false;
        }
        let handled = self.session.app_surface_host.handle_key(key);
        self.flush_app_surface_commands();
        self.sync_app_palette_actions();
        if handled {
            return true;
        }
        // Nested APP focus owns Ctrl+Tab when it implements that key. Only
        // bubble an unhandled chord to the host-level APP switcher; otherwise
        // the shell silently steals the focus navigation advertised by the APP.
        if key.code == KeyCode::Tab && key.modifiers.contains(KeyModifiers::CONTROL) {
            return self.cycle_app_panel(false);
        }
        if key.code == KeyCode::BackTab && key.modifiers.contains(KeyModifiers::CONTROL) {
            return self.cycle_app_panel(true);
        }
        false
    }

    fn handle_app_command(&mut self, command: &str) -> bool {
        if let Some(rest) = command.trim().strip_prefix("/app ") {
            let mut parts = rest.split_whitespace();
            let Some(app_id) = parts.next() else {
                return false;
            };
            let view_id = parts.next();
            let action_id = parts.next();
            let handled = match (view_id, action_id, parts.next()) {
                (_, _, Some(_)) => false,
                (None, None, None) => self.session.app_surface_host.select_app(app_id),
                (Some(view_id), None, None) => self
                    .session
                    .app_surface_host
                    .open_view(app_id, view_id, true),
                (Some(view_id), Some(action_id), None) => self
                    .session
                    .app_surface_host
                    .dispatch_action(app_id, view_id, action_id),
                _ => false,
            };
            if handled {
                self.open_sidebar_tab(TAB_APPS, "Apps");
                self.flush_app_surface_commands();
                self.sync_app_palette_actions();
            }
            return handled;
        }
        false
    }

    /// Install a canonical Runtime projection and derive only the UI view
    /// state from its live revision.  Gateway transport may reconnect or
    /// replay, but older snapshots cannot move this state backward.
    pub fn apply_execution_projection(&mut self, projection: crate::protocol::ExecutionProjection) {
        if self.app.apply_execution_projection(projection.clone()) {
            self.app
                .execution
                .turn_interaction
                .projection_snapshot(&projection);
            if self.app.execution.live_output_snapshot_gap {
                self.app.execution.turn_interaction.reconnecting();
            }
        }
    }

    pub fn apply_execution_live_update(&mut self, update: crate::protocol::ExecutionLiveUpdate) {
        if self.app.apply_execution_live_update(update) {
            let projection = self.app.execution.latest_execution_projection.clone();
            if let Some(projection) = projection.as_ref() {
                self.app
                    .execution
                    .turn_interaction
                    .projection_snapshot(projection);
            }
            if self.app.execution.live_output_snapshot_gap {
                self.app.execution.turn_interaction.reconnecting();
            }
        }
    }

    /// Fail closed for a currently selected projection.  The caller performs
    /// the generation check before invoking this method so a delayed revoke
    /// from an old stream cannot erase a newer selection.
    pub fn invalidate_execution_projection(&mut self, execution_id: &str, reason: &str) {
        if self.app.invalidate_execution_projection(execution_id) {
            self.app
                .add_system_notice(SystemNoticeKind::Warning, reason);
            self.workbench
                .runtime_activity_panel
                .sync_from_app(&self.app);
        }
    }

    // ── Rendering ───────────────────────────────────────────────

    // ── Input Handling ──────────────────────────────────────────

    /// Compatibility wrapper for tests and legacy keybinding-only callers.
    ///
    /// The production TUI event loop must use [`Self::process_raw_key`] so text
    /// editing, slash completion, dialogs, focus routing, and submissions stay
    /// on one path.
    pub fn handle_input(&mut self, event: KeyEvent) -> bool {
        self.handle_keybind_input(event)
    }

    /// Process a non-text keyboard event through the keybinding engine.
    ///
    /// If a dialog is active, the event is routed to the dialog manager
    /// first (focus trap). Otherwise, it goes through the keybind engine:
    /// - Multi-chord bindings (e.g., `g` `g`) accumulate until resolved.
    /// - Space leader key triggers which-key overlay.
    /// - Resolved actions are dispatched to the appropriate App methods.
    ///
    /// Returns `true` if the event was consumed (handled), `false` if
    /// it should propagate further.
    pub fn handle_keybind_input(&mut self, event: KeyEvent) -> bool {
        if self.overlay.command_palette.is_open() {
            if event.code == KeyCode::Esc {
                self.overlay.command_palette.close();
                return true;
            }

            let result = self
                .overlay
                .command_palette
                .handle_event(&crossterm::event::Event::Key(event));
            if result == crate::components::EventResult::Consumed {
                if let Some(action) = self.overlay.command_palette.take_action() {
                    self.dispatch_action(action);
                }
                return true;
            }
        }

        // 1. Dialog focus trap: if a dialog is active, keys go to it
        if !self.overlay.dialog_manager.is_empty() {
            return self.handle_dialog_key(&event);
        }

        // 1.5. Agent team panel focus trap: route j/k/Up/Down/Tab to panel
        if self.workbench.agent_team_panel.visible {
            if self.handle_agent_team_action(&event) {
                return true;
            }
            match event.code {
                KeyCode::Char('j' | 'k') | KeyCode::Up | KeyCode::Down => {
                    self.workbench.agent_team_panel.handle_key(&event);
                    return true;
                }
                KeyCode::Esc => {
                    self.workbench.agent_team_panel.visible = false;
                    return true;
                }
                _ => {}
            }
        }

        if self.handle_app_panel_key(event) {
            return true;
        }

        if self.handle_terminal_control_shortcut(event) {
            return true;
        }

        if self.app.shell.input.is_empty() && self.route_navigation_to_focus(event) {
            return true;
        }

        // 1.75. Tab/BackTab sidebar cycling (before keybind engine which maps Tab to no-op NextPanel)
        if self.shell.layout_state.sidebar_visible {
            match event.code {
                KeyCode::Tab => {
                    self.workbench.active_topic_panel = None;
                    self.set_focus_target(FocusTarget::Sidebar);
                    self.workbench.sidebar_active_tab =
                        (self.workbench.sidebar_active_tab + 1) % SIDEBAR_TAB_COUNT;
                    return true;
                }
                KeyCode::BackTab => {
                    self.workbench.active_topic_panel = None;
                    self.set_focus_target(FocusTarget::Sidebar);
                    self.workbench.sidebar_active_tab = if self.workbench.sidebar_active_tab == 0 {
                        SIDEBAR_TAB_COUNT - 1
                    } else {
                        self.workbench.sidebar_active_tab - 1
                    };
                    return true;
                }
                _ => {}
            }
        }

        // 1.8. Empty-input 'v' toggles the terminal display mode.
        if let KeyCode::Char('v') = event.code {
            if self.app.shell.input.is_empty()
                && !event.modifiers.contains(KeyModifiers::CONTROL)
                && !event.modifiers.contains(KeyModifiers::ALT)
            {
                self.toggle_terminal_display_mode();
                return true;
            }
        }

        // 2. Route through keybind engine
        if let Some(action) = self.shell.keybind_engine.handle_key(event) {
            self.dispatch_action(action);
            return true;
        }

        // 3. Not consumed by keybinds — may still need chord timeout check
        self.shell.keybind_engine.check_timeout();
        false
    }

    /// Full input pipeline: modal state → keybind engine → textarea fallback.
    ///
    /// Handles picker, approval, search modes. Routes text-editing keys
    /// (Ctrl+A/E/W/U/K/Z, Shift+Enter, printable chars) directly to the
    /// textarea. Everything else goes through the keybind engine.
    ///
    /// Returns the action the main loop should take in response.
    pub fn process_raw_key(&mut self, key: crossterm::event::KeyEvent) -> ProcessedKey {
        crate::performance::note_input();
        if let Some(result) = self.process_modal_key(key) {
            return result;
        }
        if let Some(result) = self.process_navigation_key(key) {
            return result;
        }
        if let Some(result) = self.process_composer_key(key) {
            return result;
        }
        self.process_control_key(key)
    }

    fn process_modal_key(&mut self, key: crossterm::event::KeyEvent) -> Option<ProcessedKey> {
        use crossterm::event::KeyCode;
        // ── Modal overlays: route keys to the topmost active overlay ──

        // 0. Message menu (Ctrl+O context menu)
        if self.shell.chat_view.pending_message_menu {
            match key.code {
                KeyCode::Char('c') => {
                    self.app.copy_focused_content();
                    self.shell.chat_view.pending_message_menu = false;
                    self.overlay.toast_manager.push(
                        ToastVariant::Success,
                        Some("Copied".into()),
                        "Entry content copied to clipboard".into(),
                        2000,
                    );
                    return Some(ProcessedKey::Nothing);
                }
                KeyCode::Char('e') => {
                    self.app.toggle_expand_current();
                    self.shell.chat_view.pending_message_menu = false;
                    return Some(ProcessedKey::Nothing);
                }
                KeyCode::Char('r') => {
                    self.shell.chat_view.pending_message_menu = false;
                    let idx = self.shell.chat_view.pending_menu_entry_idx;
                    let diff_text = String::new();
                    self.overlay.revert_dialog.open_revert_dialog(
                        &mut self.overlay.dialog_manager,
                        idx,
                        &diff_text,
                    );
                    return Some(ProcessedKey::Nothing);
                }
                KeyCode::Esc => {
                    self.shell.chat_view.pending_message_menu = false;
                    return Some(ProcessedKey::Nothing);
                }
                _ => return Some(ProcessedKey::Nothing),
            }
        }

        // 1. Command palette open → route keys to it
        if self.overlay.command_palette.is_open() {
            match key.code {
                KeyCode::Esc => {
                    self.overlay.command_palette.close();
                    return Some(ProcessedKey::Nothing);
                }
                _ => {
                    let event = crossterm::event::Event::Key(key);
                    let result = self.overlay.command_palette.handle_event(&event);
                    if result == crate::components::EventResult::Consumed {
                        if let Some(action) = self.overlay.command_palette.take_action() {
                            self.dispatch_action(action);
                        }
                    }
                    return Some(ProcessedKey::Nothing);
                }
            }
        }

        // 2. Question form active → route keys to it
        if let Some(ref mut qf) = self.overlay.question_form {
            if qf.is_active() {
                let consumed = qf.handle_key(&key);
                if consumed {
                    if qf.is_confirmed() {
                        let answers = qf.take_answers();
                        self.overlay.toast_manager.push(
                            ToastVariant::Info,
                            Some("Answers".into()),
                            format!("Received {} answers", answers.len()),
                            3000,
                        );
                    }
                    if qf.is_rejected() {
                        self.overlay.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Dismissed".into()),
                            "Question form dismissed".into(),
                            2000,
                        );
                    }
                    return Some(ProcessedKey::Nothing);
                }
            }
        }

        // 3. Export dialog active → route keys to it
        if self.overlay.export_dialog_active {
            let event = crossterm::event::Event::Key(key);
            let result = self.overlay.export_dialog.handle_event(&event);
            if result == crate::components::EventResult::Consumed {
                if let Some(ref result) = self.overlay.export_dialog.result {
                    self.overlay.pending_export_options = self.overlay.export_dialog.result.clone();
                    self.overlay.toast_manager.push(
                        ToastVariant::Success,
                        Some("Export".into()),
                        format!("Exporting to {}...", result.filename),
                        3000,
                    );
                    self.overlay.export_dialog_active = false;
                }
                if self.overlay.export_dialog.cancelled {
                    self.overlay.toast_manager.push(
                        ToastVariant::Info,
                        Some("Cancelled".into()),
                        "Export cancelled".into(),
                        2000,
                    );
                    self.overlay.export_dialog_active = false;
                }
                return Some(ProcessedKey::Nothing);
            }
        }

        if self.handle_app_panel_key(key) {
            return Some(ProcessedKey::Nothing);
        }
        None
    }

    fn process_navigation_key(&mut self, key: crossterm::event::KeyEvent) -> Option<ProcessedKey> {
        use crossterm::event::{KeyCode, KeyModifiers};
        // ── Prompt autocomplete routing (Tab / Shift+Tab / Esc) ──
        // Route these keys through the prompt component before sidebar cycling.
        // BUG 1 FIX: Sync prompt textarea from app.shell.input on-demand (not every frame).
        // This eliminates the bidirectional sync race condition.
        if self.shell.prompt.suggestions_visible() {
            match key.code {
                KeyCode::Up => {
                    self.shell.prompt.select_prev_suggestion();
                    return Some(ProcessedKey::Nothing);
                }
                KeyCode::Down => {
                    self.shell.prompt.select_next_suggestion();
                    return Some(ProcessedKey::Nothing);
                }
                _ => {}
            }
        }
        if key.code == KeyCode::Tab && !key.modifiers.contains(KeyModifiers::SHIFT) {
            let input_text = self.input_text();
            self.shell.prompt.refresh_suggestions_from_text_at_cursor(
                &input_text,
                self.input_cursor_byte_offset(),
            );
            if self.shell.prompt.suggestions_visible() {
                if let Some(new_text) = self
                    .shell
                    .prompt
                    .apply_highlighted_suggestion_to_text(&input_text)
                {
                    self.replace_input_text(&new_text);
                }
                return Some(ProcessedKey::Nothing);
            }
            // Fall through to sidebar tab cycling
        }
        if key.code == KeyCode::Tab && key.modifiers.contains(KeyModifiers::SHIFT) {
            if self.shell.prompt.suggestions_visible() {
                self.shell.prompt.select_next_suggestion();
                return Some(ProcessedKey::Nothing);
            }
            let input_text = self.input_text();
            self.shell.prompt.refresh_suggestions_from_text_at_cursor(
                &input_text,
                self.input_cursor_byte_offset(),
            );
            if self.shell.prompt.suggestions_visible() {
                return Some(ProcessedKey::Nothing);
            }
            // Fall through to sidebar tab cycling
        }
        if key.code == KeyCode::Esc {
            let event = crossterm::event::Event::Key(key);
            let result = self.shell.prompt.handle_event(&event);
            if result == crate::components::EventResult::Consumed {
                return Some(ProcessedKey::Nothing);
            }
            // Fall through to normal Esc handling
        }

        // ── Sidebar tab switching ──
        // Tab / Shift+Tab: cycle through sidebar tabs.
        if self.shell.layout_state.sidebar_visible && key.code == KeyCode::Tab {
            self.workbench.active_topic_panel = None;
            self.set_focus_target(FocusTarget::Sidebar);
            self.workbench.sidebar_active_tab =
                (self.workbench.sidebar_active_tab + 1) % SIDEBAR_TAB_COUNT;
            return Some(ProcessedKey::Nothing);
        }
        if self.shell.layout_state.sidebar_visible && key.code == KeyCode::BackTab {
            self.workbench.active_topic_panel = None;
            self.set_focus_target(FocusTarget::Sidebar);
            self.workbench.sidebar_active_tab = if self.workbench.sidebar_active_tab == 0 {
                SIDEBAR_TAB_COUNT - 1
            } else {
                self.workbench.sidebar_active_tab - 1
            };
            return Some(ProcessedKey::Nothing);
        }
        if matches!(key.code, KeyCode::Up | KeyCode::Down)
            && key.modifiers.is_empty()
            && self.shell.focus_target == FocusTarget::Input
            && (self.input_text().trim().is_empty() || self.app.shell.history_idx.is_some())
        {
            self.dispatch_action(Action::HistoryBrowse(matches!(key.code, KeyCode::Up)));
            self.set_focus_target(FocusTarget::Input);
            return Some(ProcessedKey::Nothing);
        }

        if self.app.shell.input.is_empty() && self.route_navigation_to_focus(key) {
            return Some(ProcessedKey::Nothing);
        }

        // ── Modal overrides (pick up where old input.rs left off) ──
        // 1. Picker active → route to dialog (already handled by dialog_manager in handle_input)
        // 2. Approval active → route to dialog (same)
        // 3. Search active → handle inline

        if self.app.timeline.search_active {
            return Some(self.handle_search_key(key));
        }

        if self.handle_terminal_control_shortcut(key) {
            return Some(ProcessedKey::Nothing);
        }
        None
    }

    fn process_composer_key(&mut self, key: crossterm::event::KeyEvent) -> Option<ProcessedKey> {
        use crossterm::event::{KeyCode, KeyModifiers};
        // 4. Text-editing keys → direct to textarea (bypass keybind engine)
        if self.is_textarea_key(&key) {
            self.handle_composer_edit_key(key);
            // Typing and autocomplete are transient composer state, not
            // explicit focus navigation. Announcing both on every keystroke
            // stacks toast overlays and can hide the active transcript.
            self.set_focus_target_silent(FocusTarget::Input);
            // BUG 1 FIX: Refresh suggestions from app.shell.input text, not prompt's stale textarea
            let text = self.input_text();
            self.shell
                .prompt
                .refresh_suggestions_from_text_at_cursor(&text, self.input_cursor_byte_offset());
            if self.shell.prompt.suggestions_visible() {
                self.set_focus_target_silent(FocusTarget::PromptSuggestions);
            }
            return Some(ProcessedKey::Nothing);
        }

        // 5. Enter special case: submit input or toggle expand
        if key.code == KeyCode::Enter {
            if key.modifiers.contains(KeyModifiers::SHIFT)
                || key.modifiers.contains(KeyModifiers::CONTROL)
                || key.modifiers.contains(KeyModifiers::ALT)
            {
                self.app.shell.input.insert_newline();
                return Some(ProcessedKey::Nothing);
            }
            if self.shell.prompt.suggestions_visible() {
                let input_text = self.input_text();
                if let Some(new_text) = self
                    .shell
                    .prompt
                    .apply_highlighted_suggestion_to_text(&input_text)
                {
                    if new_text != input_text {
                        self.replace_input_text(&new_text);
                        return Some(ProcessedKey::Nothing);
                    }
                    self.shell.prompt.clear_suggestions();
                } else {
                    self.shell.prompt.clear_suggestions();
                }
            }
            if self.app.shell.input.is_empty() {
                // Empty input + Enter → toggle expand on focused entry
                if let Some(entry) = self.app.timeline_get(self.app.timeline.timeline_cursor) {
                    if entry.is_collapsible() {
                        self.app.toggle_expand_current();
                        return Some(ProcessedKey::Nothing);
                    }
                }
            }
            // Non-empty input → submit
            let Some(text) = self.app.shell.input.submit_snapshot() else {
                return Some(ProcessedKey::Nothing);
            };
            if self.try_open_sidebar_for_panel_command(text.trim()) {
                self.replace_input_text("");
                return Some(ProcessedKey::Nothing);
            }
            let context_entries =
                context_entries_from_file_entries(&self.app.workbench.file_entries);
            if let Err(err) = validate_context_tokens_against_entries(&text, &context_entries) {
                self.overlay.toast_manager.push(
                    ToastVariant::Error,
                    Some("Context invalid".into()),
                    err.to_string(),
                    4000,
                );
                return Some(ProcessedKey::Nothing);
            }
            self.shell.prompt.add_history(text.clone());
            self.app.record_input_history(text.clone());
            self.app.shell.input = crate::components::composer::model::ComposerModel::default();
            return Some(ProcessedKey::Submit(text));
        }

        // 5.5 Ctrl+J: insert newline (Ctrl+Enter maps to Ctrl+J on Linux terminals)
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('j') {
            self.app.shell.input.insert_newline();
            return Some(ProcessedKey::Nothing);
        }
        None
    }

    fn process_control_key(&mut self, key: crossterm::event::KeyEvent) -> ProcessedKey {
        use crossterm::event::{KeyCode, KeyModifiers};
        // Reset pending cancel/quit on any non-ESC/Ctrl+C key
        if key.code != KeyCode::Esc
            && !(key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            self.shell.pending_cancel = false;
            self.shell.pending_quit = false;
        }

        // 6. Esc/Ctrl+C: separate cancel (Esc) from exit (Ctrl+C), both double-press
        if key.code == KeyCode::Esc {
            // Performance dashboard consumes Esc to close itself
            if self.overlay.performance_dashboard.visible {
                self.overlay.performance_dashboard.visible = false;
                self.shell.pending_cancel = false;
                self.shell.pending_quit = false;
                return ProcessedKey::Nothing;
            }
            if self.app.turn_is_active() {
                if self.shell.pending_cancel {
                    self.shell.pending_cancel = false;
                    return ProcessedKey::Cancel;
                }
                self.shell.pending_cancel = true;
                self.shell.pending_quit = false;
                self.overlay.toast_manager.push(
                    ToastVariant::Warning,
                    None,
                    "Press ESC again to cancel the current turn".into(),
                    2000,
                );
                return ProcessedKey::Nothing;
            }
            // ESC when no turn active: dismiss overlays, not exit
            if self.workbench.active_topic_panel.is_some() {
                self.workbench.active_topic_panel = None;
                self.set_focus_target(if self.shell.layout_state.sidebar_visible {
                    FocusTarget::Sidebar
                } else {
                    FocusTarget::Chat
                });
                return ProcessedKey::Nothing;
            }
            if self.workbench.activity_panel_visible {
                self.workbench.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Chat);
                return ProcessedKey::Nothing;
            }
            if self.shell.layout_state.sidebar_visible {
                self.shell
                    .layout_state
                    .toggle_sidebar(&mut self.shell.layout_tree);
                self.set_focus_target(FocusTarget::Chat);
                return ProcessedKey::Nothing;
            }
            self.shell.pending_cancel = false;
            self.shell.pending_quit = false;
            self.set_focus_target(FocusTarget::Chat);
            return ProcessedKey::Nothing;
        }
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            if self.app.turn_is_active() {
                // Ctrl+C during active turn: cancel
                if self.shell.pending_cancel {
                    self.shell.pending_cancel = false;
                    return ProcessedKey::Cancel;
                }
                self.shell.pending_cancel = true;
                self.shell.pending_quit = false;
                self.overlay.toast_manager.push(
                    ToastVariant::Warning,
                    None,
                    "Press Ctrl+C again to cancel the current turn".into(),
                    2000,
                );
                return ProcessedKey::Nothing;
            }
            // Ctrl+C when idle: exit
            if self.shell.pending_quit {
                self.shell.pending_quit = false;
                return ProcessedKey::Exit;
            }
            self.shell.pending_quit = true;
            self.shell.pending_cancel = false;
            self.overlay.toast_manager.push(
                ToastVariant::Warning,
                None,
                "Press Ctrl+C again to exit".into(),
                2000,
            );
            return ProcessedKey::Nothing;
        }

        // 7. Ctrl+V: paste from system clipboard
        if key.code == KeyCode::Char('v') && key.modifiers.contains(KeyModifiers::CONTROL) {
            match crate::clipboard::read_clipboard() {
                Some(crate::clipboard::ClipboardContent::Text(text)) => {
                    self.app.shell.input.insert_paste(&text);
                }
                Some(crate::clipboard::ClipboardContent::Image { .. }) => {
                    self.app.shell.input.insert("[Image]");
                }
                None => {}
            }
            return ProcessedKey::Nothing;
        }

        // 8. Route through keybind engine for all remaining keys
        if !self.overlay.dialog_manager.is_empty() {
            self.handle_dialog_key(&key);
            return ProcessedKey::Nothing;
        }

        if let Some(action) = self.shell.keybind_engine.handle_key(key) {
            self.dispatch_action(action);
        } else {
            self.shell.keybind_engine.check_timeout();
        }

        ProcessedKey::Nothing
    }

    /// Check if a key event should be routed directly to the textarea.
    fn is_textarea_key(&self, event: &crossterm::event::KeyEvent) -> bool {
        use crossterm::event::{KeyCode, KeyModifiers};

        // Printable characters (no modifiers or only Shift)
        if let KeyCode::Char(_) = event.code {
            if event.modifiers.is_empty() || event.modifiers == KeyModifiers::SHIFT {
                return true;
            }
            // Ctrl+A/E/W/U/K/Z → textarea for editing
            if event.modifiers == KeyModifiers::CONTROL {
                return matches!(
                    event.code,
                    KeyCode::Char('a' | 'e' | 'w' | 'u' | 'k' | 'y' | 'z')
                );
            }
            return false;
        }

        // Non-char textarea keys
        matches!(
            event.code,
            KeyCode::Backspace
                | KeyCode::Delete
                | KeyCode::Left
                | KeyCode::Right
                | KeyCode::Up
                | KeyCode::Down
                | KeyCode::Home
                | KeyCode::End
        )
    }

    fn handle_composer_edit_key(&mut self, key: KeyEvent) {
        let extend_selection = key.modifiers.contains(KeyModifiers::SHIFT);
        if key.modifiers == KeyModifiers::CONTROL {
            match key.code {
                KeyCode::Char('a') => self.app.shell.input.select_all(),
                KeyCode::Char('e') => self.app.shell.input.move_end(false),
                KeyCode::Char('w') => {
                    self.app.shell.input.delete_word_backward();
                }
                KeyCode::Char('u') => {
                    self.app.shell.input.delete_to_line_start();
                }
                KeyCode::Char('k') => {
                    self.app.shell.input.delete_to_line_end();
                }
                KeyCode::Char('z') => {
                    self.app.shell.input.undo();
                }
                KeyCode::Char('y') => {
                    self.app.shell.input.redo();
                }
                _ => {}
            }
            self.shell.composer_desired_column = None;
            return;
        }

        match key.code {
            KeyCode::Char(value) => self.app.shell.input.insert(&value.to_string()),
            KeyCode::Backspace => {
                self.app.shell.input.backspace();
            }
            KeyCode::Delete => {
                self.app.shell.input.delete_forward();
            }
            KeyCode::Left => self.app.shell.input.move_left(extend_selection),
            KeyCode::Right => self.app.shell.input.move_right(extend_selection),
            KeyCode::Home => self.app.shell.input.move_home(extend_selection),
            KeyCode::End => self.app.shell.input.move_end(extend_selection),
            KeyCode::Up => {
                self.move_composer_vertically(true, extend_selection);
                return;
            }
            KeyCode::Down => {
                self.move_composer_vertically(false, extend_selection);
                return;
            }
            _ => {}
        }
        self.shell.composer_desired_column = None;
    }

    fn move_composer_vertically(&mut self, upward: bool, extend_selection: bool) {
        let layout = crate::components::composer::layout::ComposerLayout::from_model(
            &self.app.shell.input,
            self.shell.composer_content_width,
        );
        let current_row = layout.cursor.visual_row;
        let target_row = if upward {
            current_row.checked_sub(1)
        } else {
            current_row
                .checked_add(1)
                .filter(|row| *row < layout.rows.len())
        };
        let Some(target_row) = target_row else {
            return;
        };
        let desired_column = self
            .shell
            .composer_desired_column
            .get_or_insert(layout.cursor.column);
        if let Some(byte) = layout.byte_offset_for_visual(target_row, *desired_column) {
            self.app
                .shell
                .input
                .set_cursor_byte_with_selection(byte, extend_selection);
        }
    }

    /// Insert a terminal paste/IME commit into the active text surface.
    /// Search is modal, so routing every paste to the hidden composer would
    /// make a pasted query disappear when Enter closes the search field.
    /// Normal key presses keep their existing command and shortcut routing.
    pub fn process_paste(&mut self, text: &str) {
        if self.app.timeline.search_active {
            for character in text.chars() {
                match character {
                    '\r' | '\n' | '\t' => self.app.timeline.search_query.push(' '),
                    character if !character.is_control() => {
                        self.app.timeline.search_query.push(character);
                    }
                    _ => {}
                }
            }
            self.app.request_redraw();
            return;
        }
        self.app.shell.input.insert_paste(text);
        self.shell.composer_desired_column = None;
        let input_text = self.input_text();
        self.shell
            .prompt
            .refresh_suggestions_from_text_at_cursor(&input_text, self.input_cursor_byte_offset());
        self.app.mark_dirty();
    }

    fn should_open_slash_command_palette(&self, event: &crossterm::event::KeyEvent) -> bool {
        use crossterm::event::KeyCode;

        event.code == KeyCode::Char('/')
            && event.modifiers.is_empty()
            && self.app.shell.input.text().trim().is_empty()
    }

    fn input_text(&self) -> String {
        self.app.shell.input.text().to_string()
    }

    fn input_cursor_byte_offset(&self) -> usize {
        self.app.shell.input.cursor_byte()
    }

    fn replace_input_text(&mut self, text: &str) {
        self.app.shell.input.set_text(text);
    }

    fn focus_for_current_surface(&self) -> FocusTarget {
        if self.overlay.command_palette.is_open() {
            FocusTarget::CommandPalette
        } else if !self.overlay.dialog_manager.is_empty() || self.overlay.export_dialog_active {
            FocusTarget::Dialog
        } else if self.shell.prompt.suggestions_visible() {
            FocusTarget::PromptSuggestions
        } else if let Some(topic) = self.workbench.active_topic_panel {
            FocusTarget::TopicPanel(topic)
        } else if self.shell.layout_state.sidebar_visible {
            FocusTarget::Sidebar
        } else if self.workbench.activity_panel_visible || self.app.turn_is_active() {
            FocusTarget::Activity
        } else if !self.app.shell.input.is_empty() {
            FocusTarget::Input
        } else {
            self.shell.focus_target
        }
    }

    fn set_focus_target(&mut self, target: FocusTarget) {
        if self.shell.focus_target != target {
            let label = target.label().to_string();
            let hint = target.hint().to_string();
            self.overlay.toast_manager.push(
                ToastVariant::Info,
                Some("Focus".into()),
                format!("{label}: {hint}"),
                3000,
            );
        }
        self.shell.focus_target = target;
    }

    fn set_focus_target_silent(&mut self, target: FocusTarget) {
        self.shell.focus_target = target;
    }

    fn is_navigation_key(key: &crossterm::event::KeyEvent) -> bool {
        use crossterm::event::{KeyCode, KeyModifiers};
        matches!(
            key.code,
            KeyCode::Char('j')
                | KeyCode::Char('k')
                | KeyCode::Up
                | KeyCode::Down
                | KeyCode::PageUp
                | KeyCode::PageDown
                | KeyCode::Home
                | KeyCode::End
        ) || matches!(key.code, KeyCode::Char('u' | 'd') if key.modifiers.contains(KeyModifiers::CONTROL))
    }

    fn route_navigation_to_focus(&mut self, key: crossterm::event::KeyEvent) -> bool {
        if !Self::is_navigation_key(&key) {
            return false;
        }
        let event = crossterm::event::Event::Key(key);
        match self.focus_for_current_surface() {
            FocusTarget::PromptSuggestions
            | FocusTarget::CommandPalette
            | FocusTarget::Dialog
            | FocusTarget::Input => false,
            FocusTarget::Activity => {
                if self.workbench.activity_panel.handle_event(&event)
                    == crate::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::Activity);
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Diff) => {
                if self.overlay.diff_viewer.handle_event(&event)
                    == crate::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Diff));
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Memory) => {
                if self.workbench.memory_panel.handle_event(&event)
                    == crate::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Memory));
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Skills) => {
                if self.handle_skills_panel_action(&event)
                    || self.workbench.skills_panel.handle_event(&event)
                        == crate::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Skills));
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Config) => {
                if self.handle_config_panel_action(&event)
                    || self.workbench.config_panel.handle_event(&event)
                        == crate::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Config));
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Reality) => {
                if self.handle_reality_panel_action(&event)
                    || self.workbench.reality_panel.handle_event(&event)
                        == crate::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Reality));
                    true
                } else {
                    false
                }
            }
            FocusTarget::Sidebar => self.route_navigation_to_sidebar(event),
            FocusTarget::Chat => {
                let crossterm::event::Event::Key(key) = event else {
                    return false;
                };
                match key.code {
                    crossterm::event::KeyCode::Char('j') | crossterm::event::KeyCode::Down => {
                        self.app.timeline.scroll_offset =
                            self.app.timeline.scroll_offset.saturating_add(1);
                        self.app.timeline.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::Char('k') | crossterm::event::KeyCode::Up => {
                        self.app.timeline.scroll_offset =
                            self.app.timeline.scroll_offset.saturating_sub(1);
                        self.app.timeline.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::PageDown => {
                        self.app.scroll_page_down();
                        self.app.timeline.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::PageUp => {
                        self.app.scroll_page_up();
                        self.app.timeline.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::Home => {
                        self.app.timeline.scroll_offset = 0;
                        self.app.timeline.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::End => {
                        self.app.timeline.auto_scroll = true;
                    }
                    _ => return false,
                }
                self.set_focus_target(FocusTarget::Chat);
                true
            }
        }
    }

    fn route_navigation_to_sidebar(&mut self, event: crossterm::event::Event) -> bool {
        let consumed = match self.workbench.sidebar_active_tab {
            TAB_RUNTIME => self.workbench.runtime_activity_panel.handle_event(&event),
            TAB_TOOLS => {
                if self.handle_tool_ops_action(&event) {
                    crate::components::EventResult::Consumed
                } else {
                    self.workbench.tool_ops_panel.handle_event(&event)
                }
            }
            TAB_CHANGES => self.workbench.file_changes_panel.handle_event(&event),
            TAB_GOALS => self.workbench.goal_workbench_panel.handle_event(&event),
            TAB_APPROVALS => self.workbench.approval_cockpit_panel.handle_event(&event),
            TAB_TODO => self.workbench.todo_panel.handle_event(&event),
            TAB_FILES => {
                let result = self.workbench.file_tree.handle_event(&event);
                if result == crate::components::EventResult::Consumed {
                    self.refresh_file_preview_from_gateway();
                }
                result
            }
            TAB_SESSIONS => self.session.session_sidebar.handle_event(&event),
            TAB_SURFACES => {
                if self.handle_surface_panel_action(&event) {
                    crate::components::EventResult::Consumed
                } else {
                    self.workbench.surface_panel.handle_event(&event)
                }
            }
            TAB_APPS => {
                if let crossterm::event::Event::Key(key) = event {
                    if self.handle_app_panel_key(key) {
                        crate::components::EventResult::Consumed
                    } else {
                        crate::components::EventResult::NotConsumed
                    }
                } else {
                    crate::components::EventResult::NotConsumed
                }
            }
            TAB_GATEWAY => {
                if self.handle_gateway_panel_action(&event) {
                    crate::components::EventResult::Consumed
                } else {
                    self.workbench.gateway_panel.handle_event(&event)
                }
            }
            _ => crate::components::EventResult::NotConsumed,
        } == crate::components::EventResult::Consumed;
        if consumed {
            self.set_focus_target(FocusTarget::Sidebar);
        }
        consumed
    }

    fn refresh_file_preview_from_gateway(&mut self) {
        let Some(path) = self.workbench.file_tree.selected_file_path() else {
            return;
        };
        if self.workbench.file_tree.preview_path() == Some(path.as_str()) {
            let path_for_request = path.clone();
            self.queue_gateway_api(
                move |client| async move {
                    client
                        .workspace_file_preview(&path_for_request, 64 * 1024)
                        .await
                },
                move |state, result| match result {
                    Ok(value) => {
                        let content = value
                            .get("content")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        let truncated = value
                            .get("truncated")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        let rendered = if truncated {
                            format!("{content}\n\n<preview truncated>")
                        } else {
                            content.to_string()
                        };
                        state.workbench.file_tree.apply_preview(&path, rendered);
                    }
                    Err(error) => {
                        state
                            .workbench
                            .file_tree
                            .apply_preview(&path, format!("<gateway preview error: {error}>"));
                    }
                },
            );
        }
    }

    fn handle_agent_team_action(&mut self, key: &KeyEvent) -> bool {
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }
        if key.code == KeyCode::Char('n') {
            self.workbench.agent_team_panel.select_next_team_template();
            return true;
        }
        if key.code == KeyCode::Char('t') {
            let Some(template) = self
                .workbench
                .agent_team_panel
                .selected_team_template()
                .cloned()
            else {
                self.workbench.agent_team_panel.record_action_result(
                    "team.instantiate",
                    Err("No runnable Team template is loaded".to_string()),
                );
                return true;
            };
            let objective = self.app.shell.input.text().trim().to_string();
            if objective.is_empty() {
                self.workbench.agent_team_panel.record_action_result(
                    "team.instantiate",
                    Err("Enter the Team objective in the composer before pressing t".to_string()),
                );
                return true;
            }
            let session_id = self.app.shell.session_id.clone();
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or_default();
            let team_id = format!("tui-team-{nonce}");
            let body = serde_json::json!({
                "request_id": format!("tui-team-request-{nonce}"),
                "team_id": team_id,
                "session_id": session_id,
                "selection_mode": "explicit",
                "template_selector": {
                    "kind": "latest_stable",
                    "template_id": template.template_id,
                },
                "objective": objective,
                "acceptance": template.result_fields,
                "role_binding_overrides": [],
                "cardinality_overrides": [],
                "focus_partition_plans": [],
                "permission_ceiling": "read-only",
                "model_lease": "default",
                "resource_scopes": [format!("session:{}", self.app.shell.session_id)],
            });
            self.queue_gateway_api(
                move |client| async move {
                    let mut receipt = client.instantiate_team_template(body).await?;
                    if let Some(team_id) = receipt
                        .pointer("/team/team_id")
                        .and_then(serde_json::Value::as_str)
                    {
                        let working_state = client.team_working_state(team_id).await?;
                        receipt["working_state"] = working_state;
                    }
                    Ok(receipt)
                },
                |state, result| {
                    state
                        .workbench
                        .agent_team_panel
                        .record_action_result("team.instantiate", result);
                },
            );
            return true;
        }
        let action = match key.code {
            KeyCode::Char('i') => "input",
            KeyCode::Char('!') => "interrupt",
            KeyCode::Char('X') => "shutdown",
            _ => return false,
        };
        let Some(agent_id) = self.workbench.agent_team_panel.selected_agent_id_owned() else {
            self.workbench
                .agent_team_panel
                .record_action_result(action, Err("Select an agent first".to_string()));
            return true;
        };
        let payload = serde_json::json!({
            "source": "tui.agent_team_panel",
            "session_id": self.app.shell.session_id,
            "message": "TUI operator control action"
        });
        let action_label = format!("agent.{action}");
        match action {
            "input" => self.queue_gateway_api(
                move |client| async move {
                    client.runtime_agent_input(&agent_id, payload).await
                },
                move |state, result| {
                    state
                        .workbench.agent_team_panel
                        .record_action_result(&action_label, result);
                },
            ),
            "interrupt" => self.queue_gateway_api(
                move |client| async move {
                    client.runtime_agent_interrupt(&agent_id, payload).await
                },
                move |state, result| {
                    state
                        .workbench.agent_team_panel
                        .record_action_result(&action_label, result);
                },
            ),
            "shutdown" => self.queue_gateway_api(
                move |client| async move {
                    client.runtime_agent_shutdown(&agent_id, payload).await
                },
                move |state, result| {
                    state
                        .workbench.agent_team_panel
                        .record_action_result(&action_label, result);
                },
            ),
            _ => return false,
        }
        true
    }

    fn handle_gateway_panel_action(&mut self, event: &crossterm::event::Event) -> bool {
        let crossterm::event::Event::Key(key) = event else {
            return false;
        };
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }
        self.handle_gateway_overview_key(key.code) || self.handle_gateway_review_key(key.code)
    }

    fn handle_gateway_overview_key(&mut self, code: KeyCode) -> bool {
        match code {
            KeyCode::Char('r' | 'h') => {
                self.refresh_gateway_health_panel();
                true
            }
            KeyCode::Char('e') => {
                self.queue_gateway_api(
                    move |client| async move { client.harness_eval_latest_report().await },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_harness_eval_latest(result)
                    },
                );
                true
            }
            KeyCode::Char('E') => {
                self.queue_gateway_api(
                    move |client| async move { client.harness_eval_run_smoke().await },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_action_result("harness_eval.run_smoke", result);
                    },
                );
                true
            }
            KeyCode::Char('v') => {
                self.queue_gateway_api(
                    move |client| async move { client.evolution_overview().await },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_evolution_overview(result)
                    },
                );
                true
            }
            KeyCode::Char('p') => {
                self.queue_gateway_api(
                    move |client| async move {
                        let policy = client.evolution_evaluation_policy().await?;
                        let reviews = client.evolution_evaluation_policy_reviews().await?;
                        Ok(serde_json::json!({
                            "kind": "evolution.evaluation_policy.overview",
                            "policy": policy.get("policy").cloned().unwrap_or(policy),
                            "reviews": reviews,
                        }))
                    },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_evaluation_policy_overview(result);
                    },
                );
                true
            }
            KeyCode::Char('m') => {
                self.queue_gateway_api(
                    move |client| async move { client.managed_agents().await },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_managed_agent_overview(result)
                    },
                );
                true
            }
            KeyCode::Char('D') => {
                self.queue_gateway_api(
                    move |client| async move {
                        client.dispatch_managed_agents("tui-operator", 16).await
                    },
                    |state, result| {
                        state.workbench.gateway_panel.record_action_result(
                            "runtime.managed_agents.dispatch_due_and_retry",
                            result,
                        );
                    },
                );
                true
            }
            KeyCode::Char('R') => {
                let Some(managed_agent_id) = self
                    .workbench
                    .gateway_panel
                    .selected_managed_agent_health_id()
                else {
                    self.workbench.gateway_panel.record_action_result(
                        "runtime.managed_agents.health.reset",
                        Err("no degraded Managed Agent selected; press m to refresh".to_string()),
                    );
                    return true;
                };
                self.queue_gateway_api(
                    move |client| async move {
                        client.reset_managed_agent_health(&managed_agent_id).await
                    },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_action_result("runtime.managed_agents.health.reset", result);
                    },
                );
                true
            }
            KeyCode::Char('n') => {
                self.workbench
                    .gateway_panel
                    .select_next_managed_agent_health();
                true
            }
            KeyCode::Char('N') => {
                self.workbench
                    .gateway_panel
                    .select_previous_managed_agent_health();
                true
            }
            KeyCode::Char('c') => {
                self.workbench.gateway_panel.select_next_evolution_case();
                true
            }
            KeyCode::Char('C') => {
                self.workbench
                    .gateway_panel
                    .select_previous_evolution_case();
                true
            }
            _ => false,
        }
    }

    fn handle_gateway_review_key(&mut self, code: KeyCode) -> bool {
        match code {
            KeyCode::Char('u') | KeyCode::Char('U') => {
                let analyze = matches!(code, KeyCode::Char('U'));
                let Some(case_id) = self.workbench.gateway_panel.selected_evolution_case_id()
                else {
                    self.workbench.gateway_panel.record_action_result(
                        if analyze {
                            "evolution.case.analyze"
                        } else {
                            "evolution.case.detail"
                        },
                        Err("no Ready evolution Case selected; press v to refresh".to_string()),
                    );
                    return true;
                };
                self.queue_gateway_api(
                    move |client| async move {
                        if analyze {
                            client.evolution_analyze_case(&case_id).await
                        } else {
                            client.evolution_case_detail(&case_id).await
                        }
                    },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_evolution_case_detail(result);
                    },
                );
                true
            }
            KeyCode::Char('[') => {
                self.workbench
                    .gateway_panel
                    .select_previous_release_review();
                true
            }
            KeyCode::Char(']') => {
                self.workbench.gateway_panel.select_next_release_review();
                true
            }
            KeyCode::Char('{') => {
                self.workbench.gateway_panel.select_previous_policy_review();
                true
            }
            KeyCode::Char('}') => {
                self.workbench.gateway_panel.select_next_policy_review();
                true
            }
            KeyCode::Char('a') | KeyCode::Char('x') => {
                let decision = if matches!(code, KeyCode::Char('a')) {
                    "approve"
                } else {
                    "reject"
                };
                let Some(review_id) = self.workbench.gateway_panel.selected_release_review_id()
                else {
                    self.workbench.gateway_panel.record_action_result(
                        &format!("evolution.release_review.{decision}"),
                        Err("no pending release review selected; press v to refresh".to_string()),
                    );
                    return true;
                };
                let review_id_for_request = review_id.clone();
                let decision_for_request = decision.to_string();
                let decision = decision.to_string();
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .evolution_review_decision(
                                &review_id_for_request,
                                &decision_for_request,
                                "TUI human operator decision",
                            )
                            .await
                    },
                    move |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_release_review_decision(&review_id, &decision, result);
                    },
                );
                true
            }
            KeyCode::Char('A') | KeyCode::Char('X') => {
                let decision = if matches!(code, KeyCode::Char('A')) {
                    "approve"
                } else {
                    "reject"
                };
                let Some(review_id) = self.workbench.gateway_panel.selected_policy_review_id()
                else {
                    self.workbench.gateway_panel.record_action_result(
                        &format!("evolution.evaluation_policy.{decision}"),
                        Err("no pending policy review selected; press p to refresh".to_string()),
                    );
                    return true;
                };
                let review_id_for_request = review_id.clone();
                let decision_for_request = decision.to_string();
                let decision = decision.to_string();
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .evolution_evaluation_policy_review_decision(
                                &review_id_for_request,
                                &decision_for_request,
                                "TUI human operator decision",
                            )
                            .await
                    },
                    move |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_policy_review_decision(&review_id, &decision, result);
                    },
                );
                true
            }
            KeyCode::Char('t') => {
                self.queue_gateway_api(
                    move |client| async move {
                        client.tick_mission_schedules(serde_json::json!({})).await
                    },
                    |state, result| {
                        state
                            .workbench
                            .gateway_panel
                            .record_action_result("mission.schedule.tick", result);
                    },
                );
                true
            }
            _ => false,
        }
    }

    fn handle_surface_panel_action(&mut self, event: &crossterm::event::Event) -> bool {
        let crossterm::event::Event::Key(key) = event else {
            return false;
        };
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }
        let Some(surface_id) = self.workbench.surface_panel.selected_surface_id_owned() else {
            self.workbench
                .surface_panel
                .set_status("No selected surface");
            return matches!(
                key.code,
                KeyCode::Char('h')
                    | KeyCode::Char('s')
                    | KeyCode::Char('x')
                    | KeyCode::Char('r')
                    | KeyCode::Char('R')
                    | KeyCode::Char('m')
                    | KeyCode::Char('a')
                    | KeyCode::Char('g')
                    | KeyCode::Char('i')
                    | KeyCode::Char('o')
                    | KeyCode::Char('v')
                    | KeyCode::Char('p')
                    | KeyCode::Char('d')
                    | KeyCode::Char('D')
                    | KeyCode::Char('A')
                    | KeyCode::Char('P')
            );
        };
        match key.code {
            KeyCode::Char('h') => {
                let label = format!("surface.health_check:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_health_check(&surface_id).await
                });
                true
            }
            KeyCode::Char('s') => {
                let label = format!("surface.start:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_start(&surface_id).await
                });
                true
            }
            KeyCode::Char('x') => {
                if !self
                    .workbench
                    .surface_panel
                    .require_confirmation("surface.stop", "x")
                {
                    return true;
                }
                let label = format!("surface.stop:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_stop(&surface_id).await
                });
                true
            }
            KeyCode::Char('r') => {
                if !self
                    .workbench
                    .surface_panel
                    .require_confirmation("surface.restart", "r")
                {
                    return true;
                }
                let label = format!("surface.restart:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_restart(&surface_id).await
                });
                true
            }
            KeyCode::Char('R') => {
                let label = format!("surface.repair:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_repair(&surface_id).await
                });
                true
            }
            KeyCode::Char('m') => {
                let label = format!("surface.send:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client
                        .surface_send(
                            &surface_id,
                            "tui:operator",
                            None,
                            "TUI operator ping",
                            serde_json::json!({"source": "tui.surface_panel"}),
                        )
                        .await
                });
                true
            }
            KeyCode::Char('a') => {
                let label = format!("surface.action:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client
                        .surface_action(
                            &surface_id,
                            "diagnose",
                            serde_json::json!({"source": "tui.surface_panel"}),
                        )
                        .await
                });
                true
            }
            KeyCode::Char('g') => {
                let label = format!("surface.messages:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_messages(&surface_id).await
                });
                true
            }
            KeyCode::Char('i') => {
                let label = format!("surface.inbox:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_inbox(&surface_id).await
                });
                true
            }
            KeyCode::Char('o') => {
                let label = format!("surface.outbox:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_outbox(&surface_id).await
                });
                true
            }
            KeyCode::Char('v') => {
                let label = format!("surface.deliveries:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_deliveries(&surface_id).await
                });
                true
            }
            KeyCode::Char('p') => {
                let label = format!("surface.inbox.replay:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    let inbox = client.surface_inbox(&surface_id).await?;
                    let message_id = first_surface_message_id(&inbox).ok_or_else(|| {
                        crate::gateway_client::GatewayApiError::Url(
                            "No inbox message id found".to_string(),
                        )
                    })?;
                    client.surface_replay_inbox(&surface_id, &message_id).await
                });
                true
            }
            KeyCode::Char('d') => {
                let label = format!("surface.outbox.retry:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    let outbox = client.surface_outbox(&surface_id).await?;
                    let delivery_id = first_surface_delivery_id(&outbox).ok_or_else(|| {
                        crate::gateway_client::GatewayApiError::Url(
                            "No retryable delivery id found".to_string(),
                        )
                    })?;
                    client.surface_retry_outbox(&surface_id, &delivery_id).await
                });
                true
            }
            KeyCode::Char('D') => {
                let label = format!("surface.outbox.dead_letter:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    let outbox = client.surface_outbox(&surface_id).await?;
                    let delivery_id = first_surface_delivery_id(&outbox).ok_or_else(|| {
                        crate::gateway_client::GatewayApiError::Url(
                            "No delivery id found".to_string(),
                        )
                    })?;
                    client
                        .surface_dead_letter_outbox(
                            &surface_id,
                            &delivery_id,
                            "operator moved delivery from TUI",
                        )
                        .await
                });
                true
            }
            KeyCode::Char('A') => {
                if !self
                    .workbench
                    .surface_panel
                    .require_confirmation("surface.messages.archive", "A")
                {
                    return true;
                }
                let label = format!("surface.messages.archive:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_archive_messages(&surface_id, 100).await
                });
                true
            }
            KeyCode::Char('P') => {
                if !self
                    .workbench
                    .surface_panel
                    .require_confirmation("surface.messages.purge_archived_events", "P")
                {
                    return true;
                }
                let label = format!("surface.messages.purge_archived_events:{surface_id}");
                self.queue_surface_action(label, move |client| async move {
                    client.surface_purge_archived_events(&surface_id, 100).await
                });
                true
            }
            _ => false,
        }
    }

    fn queue_surface_action<F, Fut>(&mut self, label: String, operation: F)
    where
        F: FnOnce(crate::gateway_client::GatewayApiClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<serde_json::Value, crate::gateway_client::GatewayApiError>>
            + Send
            + 'static,
    {
        self.queue_gateway_api(operation, move |state, result| {
            state
                .workbench
                .surface_panel
                .record_action_result(&label, result);
        });
    }

    fn handle_skills_panel_action(&mut self, event: &crossterm::event::Event) -> bool {
        let crossterm::event::Event::Key(key) = event else {
            return false;
        };
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }
        if key.code == KeyCode::Char('R') {
            self.refresh_skills_panel();
            return true;
        }
        let action = match key.code {
            KeyCode::Char('v') => "validate",
            KeyCode::Char('p') => "plan",
            KeyCode::Char('r') => "run",
            _ => return false,
        };
        let Some(skill_id) = self.workbench.skills_panel.selected_skill_id() else {
            self.workbench
                .skills_panel
                .record_action_result(action, Err("Select a skill first".to_string()));
            return true;
        };
        let session_id = self.app.shell.session_id.clone();
        let payload = serde_json::json!({
            "session_id": session_id,
            "reason": "tui skill panel action",
        });
        self.queue_gateway_api(
            move |client| async move { client.skill_action(&skill_id, action, payload).await },
            move |state, result| {
                state
                    .workbench
                    .skills_panel
                    .record_action_result(action, result)
            },
        );
        true
    }

    fn handle_tool_ops_action(&mut self, event: &crossterm::event::Event) -> bool {
        let crossterm::event::Event::Key(key) = event else {
            return false;
        };
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }

        match (self.workbench.tool_ops_panel.mode, key.code) {
            (_, KeyCode::Char('U')) => {
                self.refresh_tool_ops_panel_overview();
                true
            }
            (ToolOpsMode::Registry, KeyCode::Char('x')) => {
                let Some(tool_name) = self
                    .workbench
                    .tool_ops_panel
                    .selected_tool_name()
                    .map(str::to_string)
                else {
                    self.workbench
                        .tool_ops_panel
                        .set_status("No selected tool to execute");
                    return true;
                };
                self.queue_tool_ops(move |client| {
                    let name = tool_name;
                    async move {
                        client
                            .tool_execute(&name, serde_json::json!({}), "read_only")
                            .await
                    }
                });
                true
            }
            (ToolOpsMode::Operations, KeyCode::Char('i')) => {
                let prompt = self.workbench.tool_ops_panel.intent_prompt.clone();
                self.queue_tool_ops(move |client| async move {
                    client.tool_intent_plan(&prompt, Vec::new()).await
                });
                true
            }
            (ToolOpsMode::Operations, KeyCode::Char('f')) => {
                let prompt = self.workbench.tool_ops_panel.fanout_prompt.clone();
                self.queue_tool_ops(move |client| async move {
                    client.tool_context_fanout_plan(&prompt).await
                });
                true
            }
            (ToolOpsMode::Operations, KeyCode::Char('b')) => {
                let calls = match serde_json::from_str::<Vec<serde_json::Value>>(
                    &self.workbench.tool_ops_panel.batch_buffer,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.workbench
                            .tool_ops_panel
                            .set_status(format!("Invalid batch JSON: {error}"));
                        return true;
                    }
                };
                self.queue_tool_ops(move |client| async move {
                    client.tool_batch_readonly(calls, 4).await
                });
                true
            }
            (ToolOpsMode::Mutations, KeyCode::Char('v')) => {
                let edits = match serde_json::from_str::<Vec<serde_json::Value>>(
                    &self.workbench.tool_ops_panel.edits_buffer,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.workbench
                            .tool_ops_panel
                            .set_status(format!("Invalid edits JSON: {error}"));
                        return true;
                    }
                };
                self.queue_tool_ops(move |client| async move {
                    client.tool_mutation_preview(edits).await
                });
                true
            }
            (ToolOpsMode::Mutations, KeyCode::Char('A')) => {
                if !self.workbench.tool_ops_panel.arm_apply_mutation() {
                    return true;
                }
                if self.workbench.tool_ops_panel.expected_hashes.is_empty() {
                    self.workbench.tool_ops_panel.set_status(
                        "Mutation apply blocked: run preview first and verify expected hashes",
                    );
                    return true;
                }
                let edits = match serde_json::from_str::<Vec<serde_json::Value>>(
                    &self.workbench.tool_ops_panel.edits_buffer,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.workbench
                            .tool_ops_panel
                            .set_status(format!("Invalid edits JSON: {error}"));
                        return true;
                    }
                };
                let expected_hashes =
                    serde_json::to_value(&self.workbench.tool_ops_panel.expected_hashes)
                        .unwrap_or_else(|_| serde_json::json!({}));
                self.queue_tool_ops(move |client| async move {
                    client.tool_mutation_apply(edits, expected_hashes).await
                });
                true
            }
            (ToolOpsMode::Checkpoints, KeyCode::Char('n')) => {
                self.queue_gateway_api(
                    |client| async move { client.tool_checkpoint_create("tui checkpoint").await },
                    |state, result| {
                        state.record_tool_ops_result(result);
                        state.refresh_tool_ops_panel_overview();
                    },
                );
                true
            }
            (ToolOpsMode::Checkpoints, KeyCode::Char('d')) => {
                let Some(id) = self
                    .workbench
                    .tool_ops_panel
                    .selected_checkpoint_id()
                    .map(str::to_string)
                else {
                    self.workbench
                        .tool_ops_panel
                        .set_status("No selected checkpoint to diff");
                    return true;
                };
                self.queue_tool_ops(
                    move |client| async move { client.tool_checkpoint_diff(&id).await },
                );
                true
            }
            (ToolOpsMode::Checkpoints, KeyCode::Char('R')) => {
                let Some(id) = self
                    .workbench
                    .tool_ops_panel
                    .selected_checkpoint_id()
                    .map(str::to_string)
                else {
                    self.workbench
                        .tool_ops_panel
                        .set_status("No selected checkpoint to restore");
                    return true;
                };
                if !self
                    .workbench
                    .tool_ops_panel
                    .arm_restore_checkpoint(id.clone())
                {
                    return true;
                }
                self.queue_gateway_api(
                    move |client| async move { client.tool_checkpoint_restore(&id).await },
                    |state, result| {
                        state.record_tool_ops_result(result);
                        state.refresh_tool_ops_panel_overview();
                    },
                );
                true
            }
            (ToolOpsMode::Risk, KeyCode::Char('s')) => {
                let action = serde_json::json!({
                    "plane": "tui",
                    "operation": "tool_ops.simulate",
                    "actor": "tui-operator",
                    "inputs": { "mode": "risk" }
                });
                self.queue_tool_ops(move |client| async move {
                    client.cross_plane_policy_simulate(action).await
                });
                true
            }
            (ToolOpsMode::Risk, KeyCode::Char('p')) => {
                let action = serde_json::json!({
                    "actor_principal": "user:tui-operator",
                    "actor_identity_ref": null,
                    "source_channel": "tui",
                    "session_id": self.app.shell.session_id,
                    "requested_capability": "cowd.tools.operate",
                    "provider_account": null,
                    "target_ref": "tool-ops",
                    "resource_ref": null,
                    "risk": "medium",
                    "data_classification": "internal",
                    "identity_trust": "verified"
                });
                self.queue_tool_ops(move |client| async move {
                    client.preflight_cross_plane_action(action).await
                });
                true
            }
            _ => false,
        }
    }

    fn refresh_tool_ops_panel_overview(&mut self) {
        self.queue_gateway_api(
            |client| async move { client.tool_registry().await },
            |state, result| match result {
                Ok(payload) => state.workbench.tool_ops_panel.sync_registry(&payload),
                Err(error) => state
                    .workbench
                    .tool_ops_panel
                    .set_status(format!("Registry refresh failed: {error}")),
            },
        );
        self.queue_gateway_api(
            |client| async move { client.tool_cache_stats().await },
            |state, result| {
                if let Ok(payload) = result {
                    state.workbench.tool_ops_panel.sync_cache(&payload);
                }
            },
        );
        self.queue_gateway_api(
            |client| async move { client.tool_checkpoints().await },
            |state, result| {
                if let Ok(payload) = result {
                    state.workbench.tool_ops_panel.sync_checkpoints(&payload);
                }
            },
        );
        let session_id = self.app.shell.session_id.clone();
        self.queue_gateway_api(
            move |client| async move { client.runtime_timeline(&session_id, 50).await },
            |state, result| {
                if let Ok(payload) = result {
                    state.workbench.tool_ops_panel.sync_ledger(&payload);
                }
            },
        );
    }

    fn queue_tool_ops<F, Fut>(&mut self, operation: F)
    where
        F: FnOnce(crate::gateway_client::GatewayApiClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<serde_json::Value, crate::gateway_client::GatewayApiError>>
            + Send
            + 'static,
    {
        self.queue_gateway_api(operation, |state, result| {
            state.record_tool_ops_result(result);
        });
    }

    fn handle_reality_panel_action(&mut self, event: &crossterm::event::Event) -> bool {
        let crossterm::event::Event::Key(key) = event else {
            return false;
        };
        if key.kind != crossterm::event::KeyEventKind::Press
            || key.code != crossterm::event::KeyCode::Char('g')
        {
            return false;
        }
        if self.workbench.reality_panel.governance_is_running() {
            return true;
        }
        self.queue_gateway_api(
            |client| async move { client.run_memory_maintenance().await },
            |state, result| {
                state
                    .workbench
                    .reality_panel
                    .record_governance_result(result)
            },
        );
        true
    }

    fn record_tool_ops_result(&mut self, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => self.workbench.tool_ops_panel.record_receipt(payload),
            Err(error) => self
                .workbench
                .tool_ops_panel
                .set_status(format!("Tool operation failed: {error}")),
        }
    }

    pub fn handle_mouse_scroll(&mut self, down: bool) -> bool {
        self.handle_mouse_scroll_by_focus(down)
    }

    pub fn handle_mouse_scroll_at(&mut self, down: bool, x: u16, y: u16) -> bool {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let code = if down {
            KeyCode::PageDown
        } else {
            KeyCode::PageUp
        };
        let key = KeyEvent::new(code, KeyModifiers::NONE);
        let event = crossterm::event::Event::Key(key);

        if let Some(topic) = self.workbench.active_topic_panel {
            if let Some(area) = self.shell.last_hit_areas.topic {
                if TuiHitAreas::contains(area, x, y) {
                    let consumed = match topic {
                        SidebarTopicPanel::Diff => self.overlay.diff_viewer.handle_event(&event),
                        SidebarTopicPanel::Memory => {
                            self.workbench.memory_panel.handle_event(&event)
                        }
                        SidebarTopicPanel::Skills => {
                            self.workbench.skills_panel.handle_event(&event)
                        }
                        SidebarTopicPanel::Config => {
                            self.workbench.config_panel.handle_event(&event)
                        }
                        SidebarTopicPanel::Reality => {
                            self.workbench.reality_panel.handle_event(&event)
                        }
                    } == crate::components::EventResult::Consumed;
                    if consumed {
                        self.set_focus_target(FocusTarget::TopicPanel(topic));
                        return true;
                    }
                }
            }
        }

        if let Some(area) = self.shell.last_hit_areas.activity {
            if TuiHitAreas::contains(area, x, y)
                && self.workbench.activity_panel.handle_event(&event)
                    == crate::components::EventResult::Consumed
            {
                self.set_focus_target(FocusTarget::Activity);
                return true;
            }
        }

        if let Some(area) = self.shell.last_hit_areas.sidebar {
            if TuiHitAreas::contains(area, x, y) && self.route_navigation_to_sidebar(event) {
                return true;
            }
        }

        if TuiHitAreas::contains(self.shell.last_hit_areas.chat, x, y) {
            if down {
                self.app.scroll_page_down();
            } else {
                self.app.scroll_page_up();
            }
            self.app.timeline.auto_scroll = false;
            self.set_focus_target(FocusTarget::Chat);
            return true;
        }

        self.handle_mouse_scroll_by_focus(down)
    }

    fn handle_mouse_scroll_by_focus(&mut self, down: bool) -> bool {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let code = if down {
            KeyCode::PageDown
        } else {
            KeyCode::PageUp
        };
        if self.route_navigation_to_focus(KeyEvent::new(code, KeyModifiers::NONE)) {
            return true;
        }
        if down {
            self.app.scroll_page_down();
        } else {
            self.app.scroll_page_up();
        }
        self.app.timeline.auto_scroll = false;
        self.set_focus_target(FocusTarget::Chat);
        true
    }

    fn open_sidebar_tab(&mut self, tab: usize, label: &str) {
        self.workbench.activity_panel_visible = false;
        self.workbench.active_topic_panel = None;
        if !self.shell.layout_state.sidebar_visible {
            self.shell
                .layout_state
                .toggle_sidebar(&mut self.shell.layout_tree);
        }
        self.workbench.sidebar_active_tab = tab.min(SIDEBAR_TAB_COUNT.saturating_sub(1));
        match self.workbench.sidebar_active_tab {
            TAB_RUNTIME => self
                .workbench
                .runtime_activity_panel
                .clear_backlink_target(),
            TAB_APPROVALS => self
                .workbench
                .approval_cockpit_panel
                .clear_backlink_target(),
            TAB_SURFACES => self.workbench.surface_panel.clear_backlink_target(),
            _ => {}
        }
        self.set_focus_target(FocusTarget::Sidebar);
        if self.workbench.sidebar_active_tab == TAB_TOOLS {
            self.refresh_tool_ops_panel_overview();
        } else if self.workbench.sidebar_active_tab == TAB_GATEWAY {
            self.refresh_gateway_health_panel();
        } else if self.workbench.sidebar_active_tab == TAB_APPS {
            self.session.app_surface_host.activate_selected_contract();
            self.flush_app_surface_commands();
        }
        self.overlay.toast_manager.push(
            ToastVariant::Info,
            Some("Panel".into()),
            format!("Opened {label}"),
            1600,
        );
    }

    fn handle_terminal_control_shortcut(&mut self, event: KeyEvent) -> bool {
        if !event.modifiers.contains(KeyModifiers::ALT) {
            return false;
        }
        match event.code {
            KeyCode::Char('v' | 'V') => {
                self.toggle_terminal_display_mode();
                true
            }
            KeyCode::Char('e' | 'E') => {
                self.open_evidence_panorama();
                true
            }
            KeyCode::Char('g' | 'G') => {
                self.open_gateway_control_deck();
                true
            }
            _ => false,
        }
    }

    fn toggle_terminal_display_mode(&mut self) {
        self.app.shell.compact_chat = !self.app.shell.compact_chat;
        self.app.mark_dirty();
        self.shell.chat_view.mark_dirty();
        let mode = if self.app.shell.compact_chat {
            "clean"
        } else {
            "panorama"
        };
        self.overlay.toast_manager.push(
            ToastVariant::Info,
            Some("Display".into()),
            format!("Terminal mode: {mode}"),
            1500,
        );
    }

    fn open_evidence_panorama(&mut self) {
        self.app.shell.compact_chat = false;
        self.open_sidebar_tab(TAB_RUNTIME, "Evidence");
        self.workbench
            .runtime_activity_panel
            .sync_from_app(&self.app);
    }

    fn open_gateway_control_deck(&mut self) {
        self.open_sidebar_tab(TAB_GATEWAY, "Control Deck");
        self.workbench.gateway_panel.sync_from_app(&self.app);
    }

    fn open_topic_panel(&mut self, panel: SidebarTopicPanel) {
        self.workbench.activity_panel_visible = false;
        if !self.shell.layout_state.sidebar_visible {
            self.shell
                .layout_state
                .toggle_sidebar(&mut self.shell.layout_tree);
        }
        self.workbench.active_topic_panel = Some(panel);
        if panel == SidebarTopicPanel::Config {
            self.refresh_config_panel();
        } else if panel == SidebarTopicPanel::Skills {
            self.refresh_skills_panel();
        }
        self.set_focus_target(FocusTarget::TopicPanel(panel));
        self.overlay.toast_manager.push(
            ToastVariant::Info,
            Some("Panel".into()),
            format!("Opened {}", panel.label()),
            1600,
        );
    }

    fn refresh_skills_panel(&mut self) {
        self.queue_gateway_api(
            |client| async move { client.skill_projection().await },
            |state, result| match result {
                Ok(payload) => match skill_summaries_from_catalog(&payload) {
                    Ok(skills) => {
                        let count = skills.len();
                        state.app.workbench.skill_list = skills;
                        state.workbench.skills_panel.sync_from_app(&state.app);
                        state
                            .workbench
                            .skills_panel
                            .record_catalog_loaded(count, &payload);
                        state.app.mark_dirty();
                    }
                    Err(error) => {
                        state.app.workbench.skill_list.clear();
                        state.workbench.skills_panel.sync_from_app(&state.app);
                        state.workbench.skills_panel.record_catalog_failure(&error);
                    }
                },
                Err(error) => {
                    state.app.workbench.skill_list.clear();
                    state.workbench.skills_panel.sync_from_app(&state.app);
                    state.workbench.skills_panel.record_catalog_failure(&error);
                }
            },
        );
    }

    fn refresh_gateway_health_panel(&mut self) {
        self.queue_gateway_api(
            |client| async move { client.gateway_manifest().await },
            |state, result| {
                state
                    .workbench
                    .gateway_panel
                    .record_gateway_manifest(result)
            },
        );
    }

    fn try_open_sidebar_for_panel_command(&mut self, text: &str) -> bool {
        let command = text.trim();
        if command.is_empty() {
            return false;
        }

        if self.try_focus_command(command) {
            return true;
        }

        if self.handle_app_command(command) {
            return true;
        }

        if command.split_whitespace().count() != 1 {
            return false;
        }

        let Some(name) = command.strip_prefix('/') else {
            return false;
        };

        if matches!(name, "activity" | "recent") {
            if self.shell.layout_state.sidebar_visible {
                self.shell
                    .layout_state
                    .toggle_sidebar(&mut self.shell.layout_tree);
            }
            self.workbench.activity_panel_visible = !self.workbench.activity_panel_visible;
            self.set_focus_target(if self.workbench.activity_panel_visible {
                FocusTarget::Activity
            } else {
                FocusTarget::Chat
            });
            let label = if self.workbench.activity_panel_visible {
                "Activity opened"
            } else {
                "Activity hidden"
            };
            self.overlay.toast_manager.push(
                ToastVariant::Info,
                Some("Panel".into()),
                label.into(),
                1600,
            );
            return true;
        }

        let topic = match name {
            "diff" => Some(SidebarTopicPanel::Diff),
            "memory" => Some(SidebarTopicPanel::Memory),
            "skills" | "skill" => Some(SidebarTopicPanel::Skills),
            "config" | "settings" | "providers" => Some(SidebarTopicPanel::Config),
            "reality" | "facts" | "fact-flow" | "matrix" => Some(SidebarTopicPanel::Reality),
            _ => None,
        };
        if let Some(topic) = topic {
            self.open_topic_panel(topic);
            return true;
        }

        if let Some(panel) = panel_registry::find_by_alias(name) {
            if let Some(tab) = panel.sidebar_index {
                self.open_sidebar_tab(tab, panel.label);
                return true;
            }
            self.overlay.toast_manager.push(
                ToastVariant::Info,
                Some("Workbench".into()),
                format!(
                    "{} workbench is being opened through its current surface",
                    panel.label
                ),
                1800,
            );
            match panel.id {
                "config" => self.open_topic_panel(SidebarTopicPanel::Config),
                "reality" => self.open_topic_panel(SidebarTopicPanel::Reality),
                _ => {}
            }
            return true;
        }

        false
    }

    pub fn open_surface_for_slash_result(&mut self, command_name: &str) {
        match command_name {
            "runtime" | "status" | "model" | "cost" | "sandbox" | "doctor" | "context" => {
                self.open_sidebar_tab(TAB_RUNTIME, "Runtime");
            }
            "config" | "providers" => self.open_topic_panel(SidebarTopicPanel::Config),
            "memory" | "closet" => self.open_topic_panel(SidebarTopicPanel::Memory),
            "reality" | "matrix" | "fact-flow" => self.open_topic_panel(SidebarTopicPanel::Reality),
            "diff" => self.open_topic_panel(SidebarTopicPanel::Diff),
            "skills" | "skill" => self.open_topic_panel(SidebarTopicPanel::Skills),
            "tools" | "toolops" | "tool-ops" => self.open_sidebar_tab(TAB_TOOLS, "Tools"),
            "tasks" => self.open_sidebar_tab(TAB_GOALS, "Goals"),
            "approvals" => self.open_sidebar_tab(TAB_APPROVALS, "Approvals"),
            "session" | "resume" => self.open_sidebar_tab(TAB_SESSIONS, "Sessions"),
            "surfaces" | "surface" => self.open_sidebar_tab(TAB_SURFACES, "Surfaces"),
            "apps" | "app" => self.open_sidebar_tab(TAB_APPS, "Apps"),
            "gateway" => self.open_sidebar_tab(TAB_GATEWAY, "Gateway"),
            _ => {}
        }
    }

    fn try_focus_command(&mut self, command: &str) -> bool {
        let Some(rest) = command.strip_prefix("/focus") else {
            return false;
        };
        let target = rest.trim();
        if target.is_empty() {
            self.overlay.toast_manager.push(
                ToastVariant::Info,
                Some("Focus".into()),
                "Use /focus chat|input|activity|runtime|tools|files|sessions|apps|gateway|diff|memory|skills|config"
                    .into(),
                2400,
            );
            return true;
        }

        match target {
            "chat" => {
                self.workbench.active_topic_panel = None;
                self.workbench.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Chat);
            }
            "input" => {
                self.workbench.active_topic_panel = None;
                self.workbench.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Input);
            }
            "activity" | "recent" => {
                if self.shell.layout_state.sidebar_visible {
                    self.shell
                        .layout_state
                        .toggle_sidebar(&mut self.shell.layout_tree);
                }
                self.workbench.activity_panel_visible = true;
                self.set_focus_target(FocusTarget::Activity);
            }
            "sidebar" => {
                if !self.shell.layout_state.sidebar_visible {
                    self.shell
                        .layout_state
                        .toggle_sidebar(&mut self.shell.layout_tree);
                }
                self.workbench.active_topic_panel = None;
                self.workbench.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Sidebar);
            }
            "diff" => self.open_topic_panel(SidebarTopicPanel::Diff),
            "memory" => self.open_topic_panel(SidebarTopicPanel::Memory),
            "skills" | "skill" => self.open_topic_panel(SidebarTopicPanel::Skills),
            "config" | "settings" | "providers" => self.open_topic_panel(SidebarTopicPanel::Config),
            "reality" | "facts" | "fact-flow" | "matrix" => {
                self.open_topic_panel(SidebarTopicPanel::Reality)
            }
            _ => {
                if let Some(panel) = panel_registry::find_by_alias(target) {
                    if let Some(tab) = panel.sidebar_index {
                        self.open_sidebar_tab(tab, panel.label);
                    } else if panel.id == "config" {
                        self.open_topic_panel(SidebarTopicPanel::Config);
                    } else if panel.id == "reality" {
                        self.open_topic_panel(SidebarTopicPanel::Reality);
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        true
    }

    fn open_command_palette(&mut self) {
        self.refresh_command_projection_from_gateway();
        let snapshot = crate::runtime_control_store::RuntimeControlSnapshot::from_app(&self.app);
        self.overlay.command_palette.sync_runtime_actions(&snapshot);
        self.sync_app_palette_actions();
        self.overlay.command_palette.open();
        self.set_focus_target(FocusTarget::CommandPalette);
    }

    fn open_command_palette_with_query(&mut self, query: &str) {
        self.refresh_command_projection_from_gateway();
        let snapshot = crate::runtime_control_store::RuntimeControlSnapshot::from_app(&self.app);
        self.overlay.command_palette.sync_runtime_actions(&snapshot);
        self.sync_app_palette_actions();
        self.overlay.command_palette.open_with_query(query);
        self.set_focus_target(FocusTarget::CommandPalette);
    }

    fn refresh_command_projection_from_gateway(&mut self) {
        self.queue_gateway_api(
            |client| async move { client.slash_projection("tui").await },
            |state, result| {
                if let Ok(payload) = result {
                    state
                        .overlay
                        .command_palette
                        .sync_command_projection(&payload);
                    state.sync_app_palette_actions();
                    state
                        .shell
                        .prompt
                        .sync_command_suggestions_from_projection(&payload);
                }
            },
        );
    }

    /// Handle a key press while search is active.
    fn handle_search_key(&mut self, key: crossterm::event::KeyEvent) -> ProcessedKey {
        use crossterm::event::KeyCode;
        match key.code {
            KeyCode::Esc => {
                self.app.cancel_search();
                ProcessedKey::Nothing
            }
            KeyCode::Enter => {
                let query = self.app.timeline.search_query.clone();
                self.app.timeline.search_active = false;
                if !query.is_empty() {
                    self.app.execute_search(&query);
                }
                ProcessedKey::Nothing
            }
            KeyCode::Backspace => {
                self.app.timeline.search_query.pop();
                ProcessedKey::Nothing
            }
            KeyCode::Char(c) => {
                self.app.timeline.search_query.push(c);
                ProcessedKey::Nothing
            }
            _ => ProcessedKey::Nothing,
        }
    }

    // ── Dialog result polling ──────────────────────────────────

    /// Pop and return the last dialog result, if a dialog was just dismissed.
    pub fn take_dialog_result(&mut self) -> Option<crate::components::dialog::DialogResult> {
        self.overlay.dialog_manager.take_last_dismissed_result()
    }

    fn handle_dialog_key(&mut self, event: &KeyEvent) -> bool {
        let consumed = self.overlay.dialog_manager.handle_key(event);
        let Some(result) = self.take_dialog_result() else {
            return consumed;
        };
        if let Some((id, scope, can_approve)) = self.overlay.pending_approval_dialog.take() {
            let approved = matches!(result, crate::components::dialog::DialogResult::Yes);
            if (approved && can_approve)
                || matches!(
                    result,
                    crate::components::dialog::DialogResult::No
                        | crate::components::dialog::DialogResult::Cancel
                )
            {
                self.dispatch_action(Action::RespondGatewayApproval {
                    id,
                    approved,
                    scope,
                });
            }
        }
        consumed
    }

    /// Open the session picker as a Select dialog.
    pub fn open_session_picker_dialog(&mut self) {
        use crate::components::dialog::{DialogKind, DialogState};
        let items: Vec<String> = self
            .app
            .shell
            .picker_sessions
            .iter()
            .map(|s| {
                let ts = chrono::DateTime::from_timestamp((s.updated_at_ms / 1000) as i64, 0)
                    .map(|d| d.format("%m-%d %H:%M").to_string())
                    .unwrap_or_default();
                format!(
                    "{}  {} msgs  {}  {}",
                    "",
                    s.message_count,
                    ts,
                    &s.id[..8.min(s.id.len())]
                )
            })
            .collect();
        let dialog = DialogState::new(DialogKind::Select {
            title: "Select session (↑↓ jk Enter Esc)".into(),
            items,
            selected: 0,
        });
        self.overlay.dialog_manager.push(dialog);
        self.app.shell.picker_active = false; // use dialog instead of raw picker
    }

    /// Open the approval request as a Confirm dialog.
    pub fn open_approval_dialog(&mut self) {
        use crate::components::dialog::{DialogKind, DialogState};
        if let Some(req) = self.app.gateway.gateway_approval_items.first() {
            let can_approve_once = req.allowed_scopes.iter().any(|scope| scope == "once");
            let resources = if req.resources.is_empty() {
                req.resource_ref.as_deref().unwrap_or("none").to_string()
            } else {
                req.resources.join(", ")
            };
            let message = format!(
                "Request: {}\nTool: {}\nEffect: {}\nRisk: {}\nResources: {}\nPolicy revision: {}\nDeadline: {}\nSandbox: {} -> {}\nRequester: {}\nTarget/Input: {}\nAllowed scopes: {}\nSkippable: {}\n\n{} · N/Esc reject",
                req.id,
                req.tool_name,
                req.effect.as_deref().unwrap_or("unknown"),
                req.risk.as_deref().unwrap_or("unknown"),
                resources,
                req.policy_revision.map_or_else(|| "unknown".to_string(), |value| value.to_string()),
                req.expires_at_ms.map_or_else(|| "none".to_string(), |value| value.to_string()),
                req.requested_sandbox_posture.as_deref().unwrap_or("unknown"),
                req.effective_sandbox_posture.as_deref().unwrap_or("unknown"),
                req.requester.as_deref().unwrap_or("current session"),
                req.input_preview.chars().take(160).collect::<String>(),
                if req.allowed_scopes.is_empty() { "none".to_string() } else { req.allowed_scopes.join(", ") },
                req.skippable,
                if can_approve_once { "Y approve once" } else { "Approval disabled by policy" },
            );
            let dialog = DialogState::new(DialogKind::Confirm {
                title: "Approval Required".into(),
                message,
                // Approval must be an explicit affirmative action. Enter may
                // never grant a side effect by accident.
                default: false,
            });
            self.overlay.pending_approval_dialog =
                Some((req.id.clone(), "once".to_string(), can_approve_once));
            self.overlay.dialog_manager.push(dialog);
        }
    }

    // ── Action Dispatch ─────────────────────────────────────────

    /// Execute the side effects of a resolved keybinding action.
    ///
    /// Maps every [`Action`] variant to the appropriate App method call
    /// or TuiState operation.
    fn dispatch_action(&mut self, action: Action) {
        let action = match self.reduce_shell_action(action) {
            Ok(()) => return,
            Err(action) => action,
        };
        let action = match self.reduce_navigation_action(action) {
            Ok(()) => return,
            Err(action) => action,
        };
        let action = match self.reduce_approval_action(action) {
            Ok(()) => return,
            Err(action) => action,
        };
        let action = match self.reduce_task_action(action) {
            Ok(()) => return,
            Err(action) => action,
        };
        let _ = self.reduce_connector_action(action);
    }

    fn reduce_shell_action(&mut self, action: Action) -> Result<(), Action> {
        match action {
            Action::Scroll(delta) => {
                let magnitude = usize::try_from(delta.unsigned_abs()).unwrap_or(usize::MAX);
                if delta > 0 {
                    self.app.timeline.scroll_offset =
                        self.app.timeline.scroll_offset.saturating_add(magnitude);
                    self.app.timeline.auto_scroll = false;
                } else {
                    self.app.timeline.scroll_offset =
                        self.app.timeline.scroll_offset.saturating_sub(magnitude);
                    self.app.timeline.auto_scroll = false;
                }
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ScrollPage(direction) => {
                if direction > 0 {
                    self.app.scroll_page_down();
                } else {
                    self.app.scroll_page_up();
                }
                self.app.timeline.auto_scroll = false;
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ScrollTop => {
                self.app.timeline.scroll_offset = 0;
                self.app.timeline.auto_scroll = false;
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ScrollBottom => {
                self.app.timeline.auto_scroll = true;
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ExpandCollapse => {
                self.app.toggle_expand_current();
            }
            Action::Copy => {
                let focus = self.focus_for_current_surface();
                let copied = if matches!(focus, FocusTarget::Activity)
                    || (matches!(focus, FocusTarget::Sidebar)
                        && self.workbench.sidebar_active_tab == 0)
                {
                    self.workbench.runtime_activity_panel.copy_text()
                } else {
                    self.app.copy_focused_content()
                };
                if copied {
                    self.overlay.toast_manager.push(
                        ToastVariant::Success,
                        Some("Copied".into()),
                        "Focused content copied to clipboard".into(),
                        2000,
                    );
                } else {
                    self.overlay.toast_manager.push(
                        ToastVariant::Warning,
                        Some("Copy".into()),
                        "Nothing to copy".into(),
                        2000,
                    );
                }
            }
            Action::Quit => {
                self.app.shell.should_quit = true;
            }
            Action::NextPanel => {
                // Panel rotation removed — use sidebar navigation instead
            }
            Action::PrevPanel => {
                // Panel rotation removed — use sidebar navigation instead
            }
            Action::ToggleCommandPalette => {
                if self.overlay.command_palette.is_open() {
                    self.overlay.command_palette.close();
                    self.set_focus_target(FocusTarget::Chat);
                } else {
                    self.open_command_palette();
                }
            }
            Action::ToggleAgentsOverlay => {
                self.overlay.agents_overlay.toggle();
            }
            Action::ToggleAgentPanel => {
                self.workbench.agent_team_panel.toggle();
                if self.workbench.agent_team_panel.visible {
                    self.queue_gateway_api(
                        move |client| async move { client.team_templates().await },
                        |state, result| match result {
                            Ok(payload) => state
                                .workbench
                                .agent_team_panel
                                .set_team_templates(&payload),
                            Err(error) => state
                                .workbench
                                .agent_team_panel
                                .record_action_result("team.templates", Err(error)),
                        },
                    );
                }
            }
            Action::TogglePerformanceDashboard => {
                self.overlay.performance_dashboard.toggle();
            }
            Action::ToggleTheme => {
                self.app.shell.theme.toggle();
                self.shell.theme_engine.toggle_dark_light();
            }
            Action::ToggleHelp => {
                // Toggle which-key overlay via keybind engine
                if self.shell.keybind_engine.which_key_visible {
                    self.shell.keybind_engine.flush_pending();
                } else {
                    self.shell.keybind_engine.which_key_visible = true;
                }
            }
            Action::Search => {
                if self.app.shell.input.is_empty() {
                    self.app.timeline.search_active = true;
                    self.app.timeline.search_query.clear();
                    // Trigger search highlight pulse animation
                    self.shell
                        .animation_engine
                        .start_one_shot(AnimationKind::SearchPulse, 4);
                }
            }
            Action::SearchNext => {
                if self.app.shell.input.is_empty() && !self.app.timeline.search_matches.is_empty() {
                    self.app.search_next();
                    // Re-trigger pulse on each match navigation
                    self.shell
                        .animation_engine
                        .start_one_shot(AnimationKind::SearchPulse, 4);
                }
            }
            Action::SearchPrev => {
                if self.app.shell.input.is_empty() && !self.app.timeline.search_matches.is_empty() {
                    self.app.search_prev();
                    self.shell
                        .animation_engine
                        .start_one_shot(AnimationKind::SearchPulse, 4);
                }
            }
            action => return Err(action),
        }
        Ok(())
    }

    fn reduce_navigation_action(&mut self, action: Action) -> Result<(), Action> {
        match action {
            Action::Cancel => {
                // Cascade: help/which-key → search → picker → dialog → turn
                self.shell.keybind_engine.flush_pending();
                self.app.cancel_search();
                if self.app.shell.picker_active {
                    self.app.close_session_picker();
                }
                if !self.overlay.dialog_manager.is_empty() {
                    self.overlay.dialog_manager.pop();
                }
            }
            Action::SubmitInput => {
                // Handled by the input layer — no-op at dispatch level.
                // The event loop reads self.app.shell.input content separately.
            }
            Action::NextModel => {
                let previous_model = self.app.shell.model.clone();
                let previous_requested = self.app.shell.requested_model.clone();
                if let Some(model) = self.app.next_model() {
                    let session_id = self.app.shell.session_id.clone();
                    let requested_model = model.clone();
                    self.app
                        .show_notification(&format!("Requesting model switch: {requested_model}"));
                    self.queue_gateway_api(
                        move |client| async move {
                            client
                                .update_session_model(&session_id, &requested_model)
                                .await
                        },
                        move |state, result| match result {
                            Ok(_) => {
                                state.app.shell.requested_model = Some(model.clone());
                                state.app.shell.model = model.clone();
                                state.app.shell.model_dirty = false;
                                state.app.show_notification(&format!(
                                    "Session model updated: {model}; effective model will confirm on the next provider attempt"
                                ));
                            }
                            Err(error) => {
                                state.app.shell.model = previous_model.clone();
                                state.app.shell.requested_model = previous_requested.clone();
                                state.app.shell.model_dirty = false;
                                state.app.show_notification(&format!(
                                    "Model switch failed and was rolled back: {error}"
                                ));
                            }
                        },
                    );
                }
            }
            Action::RefreshConfigStatus => {
                self.refresh_config_panel();
                self.reload_runtime_provider_projection();
            }
            Action::HistoryBrowse(older) => {
                let text = if older {
                    self.app.history_prev()
                } else {
                    self.app.history_next()
                };
                if let Some(text) = text {
                    self.app.shell.input.set_text(text);
                }
            }
            Action::OpenDialog(name) => {
                use crate::components::dialog::{DialogKind, DialogState};
                match name.as_str() {
                    "command_palette" => {
                        self.open_command_palette();
                    }
                    "export" => {
                        self.overlay.export_dialog.reset();
                        self.overlay.export_dialog_active = true;
                    }
                    _ => {
                        let dialog = match name.as_str() {
                            _ => DialogState::new(DialogKind::Alert {
                                title: name.clone(),
                                message: format!("Dialog '{name}' not yet implemented."),
                            }),
                        };
                        self.overlay.dialog_manager.push(dialog);
                        self.shell
                            .animation_engine
                            .start_one_shot(AnimationKind::DialogFade, 4);
                    }
                }
            }
            Action::FocusDiff => {
                self.open_topic_panel(SidebarTopicPanel::Diff);
            }
            Action::FocusFileTree => {
                if !self.shell.layout_state.sidebar_visible {
                    self.shell
                        .layout_state
                        .toggle_sidebar(&mut self.shell.layout_tree);
                }
                self.workbench.active_topic_panel = None;
                self.workbench.sidebar_active_tab = TAB_FILES;
            }
            Action::FocusSessions => {
                if !self.shell.layout_state.sidebar_visible {
                    self.shell
                        .layout_state
                        .toggle_sidebar(&mut self.shell.layout_tree);
                }
                self.workbench.active_topic_panel = None;
                self.workbench.sidebar_active_tab = TAB_SESSIONS;
            }
            Action::Execute(ref cmd) => {
                if self.try_open_sidebar_for_panel_command(cmd) {
                    return Ok(());
                }
                self.app.shell.input.set_text(cmd);
                self.app
                    .show_notification("Command prepared. Press Enter to run.");
            }
            action => return Err(action),
        }
        Ok(())
    }

    fn reduce_approval_action(&mut self, action: Action) -> Result<(), Action> {
        match action {
            Action::RespondGatewayApproval {
                id,
                approved,
                scope,
            } => {
                if let Some(application_approval) = self
                    .app
                    .gateway
                    .gateway_approval_items
                    .iter()
                    .find(|approval| approval.id == *id)
                {
                    if application_approval.has_application_review() {
                        let app_id = application_approval
                            .application_source_id()
                            .unwrap_or_default();
                        let review_ref = application_approval
                            .review_ref
                            .as_deref()
                            .unwrap_or_default();
                        if self.handle_app_command(&format!("/{app_id} review {review_ref}")) {
                            return Ok(());
                        }
                        self.overlay.toast_manager.push(
                            ToastVariant::Error,
                            Some("Application approval".into()),
                            "The owning application review surface is unavailable; approval remains fail-closed."
                                .into(),
                            4200,
                        );
                        return Ok(());
                    }
                    if application_approval.application_source_id().is_some() {
                        self.overlay.toast_manager.push(
                            ToastVariant::Error,
                            Some("Application approval".into()),
                            "Application approval has no typed review reference; generic approval remains fail-closed."
                                .into(),
                            4200,
                        );
                        return Ok(());
                    }
                }
                let approval_id = id.clone();
                let request_id = id.clone();
                let approval_scope = scope.clone();
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .respond_approval(&request_id, approved, Some(&approval_scope), None)
                            .await
                    },
                    move |state, result| match result {
                        Ok(_) => {
                            let verdict = if approved { "approved" } else { "rejected" };
                            state.push_runtime_action_receipt(
                                "ok",
                                verdict,
                                "daemon-control",
                                "daemon.approval.respond",
                                Some(approval_id.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Success,
                                Some("Approval".into()),
                                format!("Gateway approval {verdict}"),
                                2000,
                            );
                        }
                        Err(err) => {
                            state.push_runtime_action_receipt(
                                "failed",
                                &err,
                                "daemon-control",
                                "daemon.approval.respond",
                                Some(approval_id),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Warning,
                                Some("Approval".into()),
                                err,
                                3000,
                            );
                        }
                    },
                );
            }
            Action::RevokeGatewayApprovalGrant(id) => {
                let grant_id = id.clone();
                let receipt_id = id.clone();
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .revoke_approval_grant(
                                &grant_id,
                                "revoked from the TUI approval cockpit",
                            )
                            .await
                    },
                    move |state, result| match result {
                        Ok(_) => {
                            state.push_runtime_action_receipt(
                                "ok",
                                "approval grant revoked",
                                "daemon-control",
                                "daemon.approval.grant.revoke",
                                Some(receipt_id.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Success,
                                Some("Approval".into()),
                                "Approval grant revoked".into(),
                                2000,
                            );
                        }
                        Err(error) => {
                            state.push_runtime_action_receipt(
                                "failed",
                                &error,
                                "daemon-control",
                                "daemon.approval.grant.revoke",
                                Some(receipt_id.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Error,
                                Some("Approval".into()),
                                error,
                                4200,
                            );
                        }
                    },
                );
            }
            action => return Err(action),
        }
        Ok(())
    }

    fn reduce_task_action(&mut self, action: Action) -> Result<(), Action> {
        match action {
            Action::CancelGatewayTask {
                id,
                expected_revision,
            } => {
                let task_id = id.clone();
                let request_id = id.clone();
                self.queue_gateway_api(
                    move |client| async move {
                        client.cancel_task(&request_id, expected_revision).await
                    },
                    move |state, result| match result {
                        Ok(_) => {
                            state.push_runtime_action_receipt(
                                "ok",
                                "cancelled",
                                "daemon-control",
                                "daemon.task.cancel",
                                Some(task_id.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Success,
                                Some("Task".into()),
                                "Gateway task canceled".into(),
                                2000,
                            );
                        }
                        Err(err) => {
                            state.push_runtime_action_receipt(
                                "failed",
                                &err,
                                "daemon-control",
                                "daemon.task.cancel",
                                Some(task_id),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Warning,
                                Some("Task".into()),
                                err,
                                3000,
                            );
                        }
                    },
                );
            }
            Action::CompleteGatewayTask {
                id,
                expected_revision,
            } => {
                let task_id = id.clone();
                let request_id = id.clone();
                self.queue_gateway_api(
                    move |client| async move {
                        client.complete_task(&request_id, expected_revision).await
                    },
                    move |state, result| match result {
                        Ok(_) => {
                            state.push_runtime_action_receipt(
                                "ok",
                                "completed",
                                "daemon-control",
                                "daemon.task.complete",
                                Some(task_id.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Success,
                                Some("Task".into()),
                                "Gateway task completed".into(),
                                2000,
                            );
                        }
                        Err(err) => {
                            state.push_runtime_action_receipt(
                                "failed",
                                &err,
                                "daemon-control",
                                "daemon.task.complete",
                                Some(task_id),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Warning,
                                Some("Task".into()),
                                err,
                                3000,
                            );
                        }
                    },
                );
            }
            Action::SetGatewayTaskFocus {
                session_id,
                task_id,
                expected_revision,
            } => {
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .set_session_task_focus(&session_id, &task_id, expected_revision)
                            .await
                    },
                    |state, result| state.handle_routing_focus_result("Task focus", result),
                );
            }
            Action::ClearGatewayTaskFocus {
                session_id,
                expected_revision,
            } => {
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .clear_session_task_focus(&session_id, expected_revision)
                            .await
                    },
                    |state, result| state.handle_routing_focus_result("Task focus", result),
                );
            }
            Action::SetGatewayMissionFocus {
                session_id,
                mission_id,
                expected_revision,
            } => {
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .set_session_mission_focus(&session_id, &mission_id, expected_revision)
                            .await
                    },
                    |state, result| state.handle_routing_focus_result("Mission focus", result),
                );
            }
            Action::ClearGatewayMissionFocus {
                session_id,
                expected_revision,
            } => {
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .clear_session_mission_focus(&session_id, expected_revision)
                            .await
                    },
                    |state, result| state.handle_routing_focus_result("Mission focus", result),
                );
            }
            action => return Err(action),
        }
        Ok(())
    }

    fn reduce_connector_action(&mut self, action: Action) -> Result<(), Action> {
        match action {
            Action::RevalidateConnectorResource { reference, state } => {
                let resource_ref = reference.clone();
                let desired_state = state.clone();
                let request_ref = reference.clone();
                let request_state = state.clone();
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .revalidate_connector_resource(&request_ref, &request_state)
                            .await
                    },
                    move |state, result| match result {
                        Ok(value)
                            if value.get("ok").and_then(serde_json::Value::as_bool)
                                == Some(true) =>
                        {
                            state.apply_local_connector_resource_state(
                                &resource_ref,
                                &desired_state,
                            );
                            state.push_runtime_action_receipt(
                                "ok",
                                &desired_state,
                                "daemon-control",
                                "connector.resource.revalidate",
                                Some(resource_ref.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Success,
                                Some("Connector".into()),
                                format!("Resource marked {desired_state}"),
                                2000,
                            );
                        }
                        Ok(value) => {
                            let reason = value
                                .get("reason")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("resource state unchanged")
                                .to_string();
                            state.push_runtime_action_receipt(
                                "skipped",
                                &reason,
                                "daemon-control",
                                "connector.resource.revalidate",
                                Some(resource_ref),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Warning,
                                Some("Connector".into()),
                                reason,
                                3000,
                            );
                        }
                        Err(err) => {
                            state.push_runtime_action_receipt(
                                "failed",
                                &err,
                                "daemon-control",
                                "connector.resource.revalidate",
                                Some(resource_ref),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Warning,
                                Some("Connector".into()),
                                err,
                                3000,
                            );
                        }
                    },
                );
            }
            Action::PromoteConnectorResourceToMemory {
                reference,
                session_id,
            } => {
                let session_id = session_id
                    .clone()
                    .or_else(|| Some(self.app.shell.session_id.clone()));
                let receipt_ref = reference.clone();
                let request_ref = reference.clone();
                self.queue_gateway_api(
                    move |client| async move {
                        client
                            .promote_connector_resource_to_memory(
                                &request_ref,
                                session_id.as_deref(),
                            )
                            .await
                    },
                    move |state, result| match result {
                        Ok(value)
                            if value.get("ok").and_then(serde_json::Value::as_bool)
                                == Some(true) =>
                        {
                            let memory_id = value
                                .get("memory_id")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("remembered");
                            state.push_runtime_action_receipt(
                                "ok",
                                memory_id,
                                "daemon-control",
                                "connector.resource.promote_memory",
                                Some(receipt_ref.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Success,
                                Some("Memory".into()),
                                "Connector resource remembered".into(),
                                2000,
                            );
                        }
                        Ok(value) => {
                            let reason = value
                                .get("reason")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("memory promotion skipped")
                                .to_string();
                            state.push_runtime_action_receipt(
                                "skipped",
                                &reason,
                                "daemon-control",
                                "connector.resource.promote_memory",
                                Some(receipt_ref.clone()),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Warning,
                                Some("Memory".into()),
                                reason,
                                3000,
                            );
                        }
                        Err(err) => {
                            state.push_runtime_action_receipt(
                                "failed",
                                &err,
                                "daemon-control",
                                "connector.resource.promote_memory",
                                Some(receipt_ref),
                            );
                            state.overlay.toast_manager.push(
                                ToastVariant::Warning,
                                Some("Memory".into()),
                                err,
                                3000,
                            );
                        }
                    },
                );
            }
            Action::TogglePanel(ref name) if name == "sidebar" => {
                self.shell
                    .layout_state
                    .toggle_sidebar(&mut self.shell.layout_tree);
                self.workbench.active_topic_panel = None;
                self.set_focus_target(if self.shell.layout_state.sidebar_visible {
                    FocusTarget::Sidebar
                } else {
                    FocusTarget::Chat
                });
                let message = if self.shell.layout_state.sidebar_visible {
                    "Sidebar opened"
                } else {
                    "Sidebar hidden"
                };
                self.overlay.toast_manager.push(
                    ToastVariant::Info,
                    Some("Layout".into()),
                    message.into(),
                    1600,
                );
            }
            Action::TogglePanel(ref _name) => {}
            Action::ApplyPreset(preset) => {
                self.shell.layout_tree.apply_preset(preset);
                self.shell.layout_state = LayoutState::default();
                let label = match preset {
                    crate::layout::LayoutPreset::Coding => "Coding",
                    crate::layout::LayoutPreset::Review => "Review",
                    crate::layout::LayoutPreset::Collaboration => "Collaboration",
                };
                self.overlay.toast_manager.push(
                    ToastVariant::Info,
                    Some("Layout".into()),
                    format!("Switched to {label} layout"),
                    2000,
                );
            }
            Action::Noop => {}
            action => return Err(action),
        }
        Ok(())
    }

    fn apply_local_connector_resource_state(&mut self, reference: &str, state: &str) {
        self.mutate_runtime_control_store(|store| {
            store.apply_connector_resource_state(reference, state);
        });
    }

    fn handle_routing_focus_result(
        &mut self,
        label: &str,
        result: Result<serde_json::Value, String>,
    ) {
        match result {
            Ok(_) => {
                self.push_runtime_action_receipt(
                    "ok",
                    "applied",
                    "daemon-control",
                    "daemon.session.routing_focus",
                    None,
                );
                self.overlay.toast_manager.push(
                    ToastVariant::Success,
                    Some(label.into()),
                    "Routing focus updated".into(),
                    2000,
                );
            }
            Err(error) => {
                self.push_runtime_action_receipt(
                    "failed",
                    &error,
                    "daemon-control",
                    "daemon.session.routing_focus",
                    None,
                );
                self.overlay.toast_manager.push(
                    ToastVariant::Warning,
                    Some(label.into()),
                    error,
                    3600,
                );
            }
        }
    }

    fn push_runtime_action_receipt(
        &mut self,
        status: &str,
        dispatch_status: &str,
        mode: &str,
        capability: &str,
        idempotency_key: Option<String>,
    ) {
        self.mutate_runtime_control_store(|store| {
            store.push_action_receipt(status, dispatch_status, mode, capability, idempotency_key);
        });
    }

    fn mutate_runtime_control_store(
        &mut self,
        mutate: impl FnOnce(&mut crate::runtime_control_store::RuntimeControlLocalStore),
    ) {
        let mut store = crate::runtime_control_store::RuntimeControlLocalStore::from_app(&self.app);
        mutate(&mut store);
        store.apply_to_app(&mut self.app);
        self.sync_runtime_control_surfaces(store.snapshot());
    }

    fn sync_runtime_control_surfaces(
        &mut self,
        snapshot: &crate::runtime_control_store::RuntimeControlSnapshot,
    ) {
        self.workbench
            .approval_cockpit_panel
            .sync_from_app(&self.app);
        self.workbench.goal_workbench_panel.sync_from_app(&self.app);
        self.workbench.gateway_panel.sync_from_app(&self.app);
        self.workbench.surface_panel.sync_from_app(&self.app);
        self.overlay.command_palette.sync_runtime_actions(snapshot);
    }

    fn reload_runtime_provider_projection(&mut self) -> bool {
        let provider_count = self.app.gateway.gateway_connector_accounts.len();
        let provider_model_count = self.app.shell.available_models.len();
        self.workbench
            .runtime_activity_panel
            .sync_from_app(&self.app);
        let message = format!(
            "Provider projection refreshed: {provider_count} accounts, {provider_model_count} models"
        );
        self.overlay.toast_manager.push(
            ToastVariant::Info,
            Some("Providers".into()),
            message.clone(),
            3000,
        );
        self.app.show_notification(&message);
        true
    }

    fn refresh_config_panel(&mut self) -> bool {
        self.workbench.config_panel.set_status("Refreshing config…");
        self.queue_gateway_api(
            |client| async move {
                let config = client.config().await?;
                let providers = client.config_providers().await?;
                let effective = client.runtime_effective_config().await?;
                let reload_status = client.config_reload_status().await?;
                Ok(serde_json::json!({
                    "config": config,
                    "providers": providers,
                    "effective": effective,
                    "reload_status": reload_status,
                }))
            },
            |state, result| match result {
                Ok(payload) => {
                    state.workbench.config_panel.sync_config(
                        payload.get("config").cloned().unwrap_or_default(),
                        payload.get("providers").cloned().unwrap_or_default(),
                        payload.get("effective").cloned().unwrap_or_default(),
                    );
                    state.workbench.config_panel.sync_config_reload_status(
                        payload.get("reload_status").cloned().unwrap_or_default(),
                    );
                    state
                        .workbench
                        .config_panel
                        .set_status("Config projection refreshed");
                }
                Err(error) => state
                    .workbench
                    .config_panel
                    .set_status(format!("Config refresh failed: {error}")),
            },
        );
        true
    }

    fn handle_config_panel_action(&mut self, event: &crossterm::event::Event) -> bool {
        let crossterm::event::Event::Key(key) = event else {
            return false;
        };
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }
        match key.code {
            KeyCode::Char('e') => self.refresh_config_panel(),
            KeyCode::Char('r') => self.refresh_config_panel(),
            KeyCode::Enter => {
                let Some(model) = self.workbench.config_panel.selected_model_id() else {
                    self.workbench.config_panel.set_status("No model selected");
                    return true;
                };
                self.queue_gateway_api(
                    move |client| async move { client.update_config_model(&model).await },
                    |state, result| {
                        state
                            .workbench
                            .config_panel
                            .record_action_result("config.model.update", result);
                        state.refresh_config_panel();
                    },
                );
                true
            }
            _ => false,
        }
    }

    // ── Convenience Methods ─────────────────────────────────────

    /// Register a component with the event dispatcher.
    ///
    /// Takes an `EventComponentId` (from the event module) which wraps
    /// a `String` identifier. Use `EventComponentId("my_component".into())`
    /// to create one.
    ///
    /// Shortcut for `self.shell.event_dispatcher.register(id, component)`.
    pub fn register_component(&mut self, id: EventComponentId, component: Box<dyn Component>) {
        self.shell.event_dispatcher.register(id, component);
    }

    /// Drain the event bus and dispatch all pending events.
    ///
    /// Shortcut for `self.shell.event_dispatcher.dispatch(&self.shell.event_bus)`.
    pub fn dispatch_events(&mut self) {
        self.shell.event_dispatcher.dispatch(&self.shell.event_bus);
    }

    /// Flush the keybind engine's pending chord (e.g., on Escape).
    pub fn flush_chord(&mut self) {
        self.shell.keybind_engine.flush_pending();
    }

    /// Check and apply keybind chord timeout.
    pub fn check_keybind_timeout(&mut self) {
        self.shell.keybind_engine.check_timeout();
    }

    /// Poll-based hot-reload for the theme engine.
    ///
    /// Returns `true` if the theme file changed and was reloaded.
    pub fn hot_reload_theme(&mut self) -> bool {
        self.shell.theme_engine.hot_reload()
    }

    // ── Startup Loading ─────────────────────────────────────────

    /// Update the startup phase based on the `ready` signal and elapsed time.
    ///
    /// Called each frame from the render cycle. Delegates to
    /// `update_startup_phase_at` with `Instant::now()`.
    pub fn update_startup_phase(&mut self, ready: bool) {
        self.update_startup_phase_at(ready, Instant::now());
    }

    /// Time-controllable variant of `update_startup_phase` for testing.
    fn update_startup_phase_at(&mut self, ready: bool, now: Instant) {
        use self::StartupPhase::*;

        const SHOW_DELAY: Duration = Duration::from_millis(STARTUP_SHOW_DELAY_MS);
        const MIN_DISPLAY: Duration = Duration::from_millis(STARTUP_MIN_DISPLAY_MS);

        match self.shell.startup_phase {
            Done => {}
            Finishing => {
                if ready {
                    if let Some(show_time) = self.shell.startup_show_time {
                        if now.duration_since(show_time) >= MIN_DISPLAY {
                            self.shell.startup_phase = Done;
                        }
                    }
                } else {
                    self.shell.startup_phase = Loading;
                    self.shell.startup_show_time = None;
                }
            }
            Loading => {
                if ready {
                    self.shell.startup_phase = Finishing;
                    self.shell.startup_show_time = Some(now);
                }
            }
            Hidden => {
                if ready {
                    // Completed before show delay → never show overlay
                    self.shell.startup_phase = Done;
                } else if now.duration_since(self.shell.startup_start) >= SHOW_DELAY {
                    self.shell.startup_phase = Loading;
                }
            }
        }
    }
}

fn skill_summaries_from_catalog(payload: &serde_json::Value) -> Result<Vec<SkillSummary>, String> {
    let items = payload
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "Gateway skill catalog has no items array".to_string())?;
    items
        .iter()
        .map(|item| {
            let required = |field: &str| {
                item.get(field)
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| format!("Gateway skill catalog item is missing {field}"))
            };
            let id = required("id")?;
            let name = required("name")?;
            let status = required("status")?;
            let scope = required("scope")?;
            let domain = item
                .get("domain")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| scope.clone());
            Ok(SkillSummary {
                id,
                name,
                description: item
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                installed: !matches!(
                    status.to_ascii_lowercase().as_str(),
                    "disabled" | "invalid" | "unavailable" | "failed"
                ),
                category: domain,
                source: required("source")?,
                status,
                risk: required("risk")?,
                tags: item
                    .get("tags")
                    .and_then(serde_json::Value::as_array)
                    .map(|tags| {
                        tags.iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn runtime_backlink_object_matches_target(target: &str, object: &serde_json::Value) -> bool {
    let expected = target
        .split_once("://")
        .map(|(_, value)| value.split(['/', '?', '#']).next().unwrap_or_default())
        .unwrap_or_default();
    if expected.is_empty() {
        return false;
    }
    let observed = object
        .get("execution_id")
        .or_else(|| object.get("task_id"))
        .or_else(|| object.get("id"))
        .or_else(|| {
            object
                .get("execution")
                .and_then(|value| value.get("execution_id"))
        })
        .and_then(serde_json::Value::as_str);
    observed == Some(expected)
}

/// Exact approval routes may return either a live runtime request
/// (`approval_id`) or a persisted history record (`id`/`request_id`).  A
/// backlink is an object identity, not a request to render whichever approval
/// happened to arrive first, so accept only records that name the target.
fn approval_backlink_object_matches_target(target: &str, object: &serde_json::Value) -> bool {
    let expected = canonical_backlink_target_id(target, "approval://");
    let Some(expected) = expected else {
        return false;
    };
    ["approval_id", "id", "request_id"]
        .into_iter()
        .any(|field| {
            object
                .get(field)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == expected)
        })
}

fn evidence_backlink_object_matches_target(target: &str, object: &serde_json::Value) -> bool {
    let expected = target
        .strip_prefix("evidence://matrix/")
        .or_else(|| target.strip_prefix("evidence://"))
        .map(|value| value.split(['/', '?', '#']).next().unwrap_or_default())
        .filter(|value| !value.is_empty());
    let Some(expected) = expected else {
        return false;
    };
    fn contains_identity(value: &serde_json::Value, expected: &str) -> bool {
        match value {
            serde_json::Value::Object(object) => {
                ["packet_id", "evidence_id", "id", "ref"]
                    .into_iter()
                    .any(|field| {
                        object
                            .get(field)
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|value| {
                                value == expected
                                    || value.ends_with(&format!("://matrix/{expected}"))
                                    || value.ends_with(&format!(":{expected}"))
                            })
                    })
                    || object
                        .values()
                        .any(|value| contains_identity(value, expected))
            }
            serde_json::Value::Array(values) => values
                .iter()
                .any(|value| contains_identity(value, expected)),
            _ => false,
        }
    }
    contains_identity(object, expected)
}

/// Surface backlinks span two exact object planes: cross-plane receipts and
/// per-surface outbox/message records.  Validate both the object identity and
/// the surface namespace before allowing a response to replace the focused
/// panel state.
fn surface_backlink_receipt_matches_target(target: &str, receipt: &serde_json::Value) -> bool {
    if let Some(expected) = canonical_backlink_target_id(target, "receipt://cross-plane/") {
        return receipt
            .get("id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value == expected);
    }

    let Some(surface_target) = target.strip_prefix("surface://") else {
        return false;
    };
    let mut parts = surface_target.splitn(3, '/');
    let surface_id = parts.next().unwrap_or_default();
    let object_kind = parts.next().unwrap_or_default();
    let object_id = parts
        .next()
        .unwrap_or(object_kind)
        .split(['?', '#'])
        .next()
        .unwrap_or_default();
    if surface_id.is_empty() || object_id.is_empty() {
        return false;
    }
    let surface_matches = receipt
        .get("surface")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| value == surface_id);
    if !surface_matches {
        return false;
    }
    let id_fields: &[&str] = if object_kind == "delivery" {
        &["delivery_id"]
    } else {
        &["message_id", "id"]
    };
    id_fields.iter().any(|field| {
        receipt
            .get(*field)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value == object_id)
    })
}

fn canonical_backlink_target_id<'a>(target: &'a str, prefix: &str) -> Option<&'a str> {
    target
        .strip_prefix(prefix)
        .map(|value| value.split(['/', '?', '#']).next().unwrap_or_default())
        .filter(|value| !value.is_empty())
}

fn context_entries_from_file_entries(entries: &[crate::FileEntry]) -> Vec<ContextWorkspaceEntry> {
    entries
        .iter()
        .map(|entry| ContextWorkspaceEntry::new(entry.name.clone(), entry.is_dir))
        .collect()
}

// ── Startup Phase ──────────────────────────────────────────────

/// Tracks the TUI startup loading overlay state machine.
///
/// - `Hidden`: startup just began, waiting for 500ms show delay
/// - `Loading`: show delay elapsed, displaying "Loading..." overlay
/// - `Finishing`: startup ready, displaying "Finishing startup..." (min 3s)
/// - `Done`: overlay hidden, startup fully complete
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartupPhase {
    #[default]
    Hidden,
    Loading,
    Finishing,
    Done,
}

const STARTUP_SHOW_DELAY_MS: u64 = 500;
const STARTUP_MIN_DISPLAY_MS: u64 = 3000;

// ── L4MemoryView ─────────────────────────────────────────────────

/// Displays L4 (shared/team-scoped) memory entries in the overlay layer.
///
/// Synced from `MemoryOrchestrator` each render frame when available.
/// Shows a compact list of recent L4 memory entries with title, tags,
/// and confidence.
pub struct L4MemoryView {
    /// Cached L4 entry titles (synced from orchestrator).
    pub entries: Vec<String>,
    /// Whether the view has been synced at least once.
    pub synced: bool,
    /// Status message (e.g. "Orchestrator available" / "No L4 entries").
    pub status: String,
    /// Last time entries were refreshed from the memory store.
    last_sync_at: Option<Instant>,
}

impl L4MemoryView {
    /// Create a new empty L4MemoryView.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            synced: false,
            status: String::new(),
            last_sync_at: None,
        }
    }

    /// Sync from the Gateway memory projection cached on the app state.
    pub fn sync_from_app(&mut self, app: &App) {
        let should_sync = self
            .last_sync_at
            .map(|last| last.elapsed() >= Duration::from_secs(1))
            .unwrap_or(true);
        if !should_sync {
            return;
        }
        self.entries = app
            .workbench
            .memory_entries
            .iter()
            .filter(|entry| entry.layer.eq_ignore_ascii_case("l4"))
            .take(40)
            .map(|entry| format!("{} {}", entry.priority, entry.content))
            .collect();
        self.status = if self.entries.is_empty() {
            "No L4 projection entries".to_string()
        } else {
            format!("Projected {} L4 entries", self.entries.len())
        };
        self.synced = !self.entries.is_empty();
        self.last_sync_at = Some(Instant::now());
    }

    /// Render the L4 memory view as a compact overlay.
    pub fn render(&self, ctx: &mut crate::components::RenderContext, area: ratatui::layout::Rect) {
        use ratatui::style::{Color, Modifier, Style};
        use ratatui::text::{Line, Span};
        use ratatui::widgets::{Block, Borders, Paragraph};

        let block = Block::default()
            .title(" L4 Memory ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        let mut lines: Vec<Line> = Vec::new();

        if self.entries.is_empty() {
            lines.push(Line::from(Span::styled(
                if self.synced {
                    "No L4 entries yet."
                } else {
                    "Orchestrator not available."
                },
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for entry in self.entries.iter().take(8) {
                lines.push(Line::from(Span::styled(
                    format!(" • {entry}"),
                    Style::default().fg(Color::White),
                )));
            }
            if self.entries.len() > 8 {
                lines.push(Line::from(Span::styled(
                    format!("... {} more", self.entries.len() - 8),
                    Style::default().fg(Color::DarkGray),
                )));
            }
        }

        if !self.status.is_empty() {
            lines.push(Line::from(Span::styled(
                &self.status,
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::DIM),
            )));
        }

        let width = area.width.min(40);
        let height = crate::components::base::terminal_len(lines.len())
            .saturating_add(2)
            .min(area.height);
        let rect = ratatui::layout::Rect::new(
            area.x.saturating_add(area.width.saturating_sub(width)),
            area.y,
            width,
            height,
        );

        let paragraph = Paragraph::new(lines).block(block);
        ctx.frame_mut().render_widget(paragraph, rect);
    }
}

fn first_surface_message_id(value: &serde_json::Value) -> Option<String> {
    first_string_field(
        value,
        &["message_id", "id"],
        &["inbox", "messages", "items"],
    )
}

fn first_surface_delivery_id(value: &serde_json::Value) -> Option<String> {
    first_string_field(
        value,
        &["delivery_id"],
        &["outbox", "dead_letters", "items"],
    )
}

fn first_string_field(
    value: &serde_json::Value,
    fields: &[&str],
    containers: &[&str],
) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for field in fields {
                if let Some(value) = map
                    .get(*field)
                    .and_then(serde_json::Value::as_str)
                    .filter(|item| !item.trim().is_empty())
                {
                    return Some(value.to_string());
                }
            }
            for container in containers {
                if let Some(found) = map
                    .get(*container)
                    .and_then(|value| first_string_field(value, fields, containers))
                {
                    return Some(found);
                }
            }
            map.values()
                .find_map(|value| first_string_field(value, fields, containers))
        }
        serde_json::Value::Array(items) => items
            .iter()
            .find_map(|value| first_string_field(value, fields, containers)),
        _ => None,
    }
}

impl Default for L4MemoryView {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "tests/render.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/authority.rs"]
mod authority_tests;
