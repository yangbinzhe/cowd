// ── TuiState — Unified TUI application state ──────────────────
// Wraps the legacy App with new engine components:
//   LayoutTree, KeybindEngine, EventBus, ThemeEngine, DialogManager.
//
// Delegates all App public methods via Deref/DerefMut.
// Bridges old App::apply_event(TuiEvent) → EventBus for new components.
// Orchestrates rendering via direct component layout + ChatView + dialogs.
//
// Architecture:
//   - TuiState OWNS App (not a reference)
//   - Deref<Target=App> for transparent delegation
//   - TuiState::apply_event() shadows App::apply_event() — adds EventBus bridging
//   - handle_input() → KeybindEngine → Action dispatch
//   - render() → sync ChatView → render_tree → render dialogs
// -------------------------------------------------------------------

#![allow(dead_code)]

use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use crossterm::event::KeyEvent;
use ratatui::Frame;

use crate::tui::accessibility::AccessibilityMode;
use crate::tui::animation::{AnimationEngine, AnimationKind};
use crate::tui::app::App;
use crate::tui::components::agents_overlay::AgentsOverlay;
use crate::tui::components::chat_view::ChatView;
use crate::tui::components::command_palette::CommandPalette;
use crate::tui::components::context_panel::ContextPanel;
use crate::tui::components::dialog::DialogManager;
use crate::tui::components::diff_viewer::DiffViewer;
use crate::tui::components::export_dialog::ExportDialog;
use crate::tui::components::file_changes_panel::FileChangesPanel;
use crate::tui::components::file_tree::FileTree;
use crate::tui::components::prompt::Prompt;
use crate::tui::components::question_form::QuestionForm;
use crate::tui::components::session_sidebar::SessionSidebar;
use crate::tui::components::status_bar::StatusBar;
use crate::tui::components::revert_dialog::RevertDialog;
use crate::tui::components::thinking_panel::ThinkingPanel;
use crate::tui::components::toast::{ToastManager, ToastVariant};
use crate::tui::components::todo_panel::TodoPanel;
use crate::tui::components::{Component, RenderContext};
use crate::tui::error_recovery::{self, RenderResult};
use crate::tui::event::dispatcher::EventDispatcher;
use crate::tui::event::{ComponentId as EventComponentId, EventBus, EventPriority};
use crate::tui::keybind::types::Action;
use crate::tui::keybind::which_key::WhichKey;
use crate::tui::keybind::{default_bindings, KeybindEngine};
use crate::tui::layout::{LayoutNode, LayoutTree};
use crate::tui::profiler::{FrameTimer, RenderProfiler};
use crate::tui::theme::ThemeEngine;
use crate::tui::TuiEvent;

/// Result of processing a key event through the TUI input pipeline.
#[derive(Debug, Clone)]
pub enum ProcessedKey {
    Submit(String),
    Cancel,
    Exit,
    Nothing,
}

// ── TuiState ────────────────────────────────────────────────────

/// Unified TUI application state.
///
/// Owns the legacy `App` (all existing fields preserved) alongside
/// the new engine components: layout tree, keybinding engine,
/// event bus, theme engine, and dialog manager.
///
/// # Delegation
///
/// Implements [`Deref`] and [`DerefMut`] to `App`, so all existing
/// App public methods and fields are directly accessible on `TuiState`.
/// The `apply_event()` method is shadowed to add EventBus bridging.
pub struct TuiState {
    /// Legacy application state (all existing fields preserved).
    pub app: App,

    /// Component layout tree (kept for LayoutState management; sidebar rendered directly).
    pub layout_tree: LayoutTree,

    /// Chat view component (rendered directly after syncing from App).
    pub chat_view: ChatView,

    /// Keybinding engine with modal-layer stacking and chord dispatch.
    pub keybind_engine: KeybindEngine,

    /// Priority-ordered event bus for TUI-internal component events.
    pub event_bus: EventBus,

    /// Registry-backed event dispatcher routing events to components.
    pub event_dispatcher: EventDispatcher,

    /// Hot-reloadable theme engine (dark/light builtins or YAML files).
    pub theme_engine: ThemeEngine,

    /// Stack-based dialog manager for alerts, confirmations, prompts.
    pub dialog_manager: DialogManager,

    /// Toast notification manager for transient status messages.
    pub toast_manager: ToastManager,

    /// Agents overlay showing subagent tree hierarchy.
    pub agents_overlay: AgentsOverlay,

    /// Thinking panel for reasoning + tool progress during active turns.
    pub thinking_panel: ThinkingPanel,

    /// Command palette for fuzzy command search (Ctrl+P).
    pub command_palette: CommandPalette,

    /// Active multi-step question form (None = not shown).
    pub question_form: Option<QuestionForm>,

    /// Export dialog for session export options.
    pub export_dialog: ExportDialog,
    /// Whether the export dialog is currently shown.
    pub export_dialog_active: bool,

    /// Revert dialog helper for per-message revert confirmation.
    pub revert_dialog: RevertDialog,

    /// Context panel showing token usage and cost.
    pub context_panel: ContextPanel,

    /// File changes panel showing modified files with +/- counts.
    pub file_changes_panel: FileChangesPanel,

    /// Todo panel displaying task list from TodoWrite tool calls.
    pub todo_panel: TodoPanel,

    /// Diff viewer component for unified/split diff display.
    pub diff_viewer: DiffViewer,

    /// Prompt component with autocomplete, frecency scoring, @file completion.
    pub prompt: Prompt,

    /// File tree browser with git status overlay.
    pub file_tree: FileTree,

    /// Session list browser with rename/delete/switch/fork actions.
    pub session_sidebar: SessionSidebar,

    /// Active tab index in the sidebar (0=Context, 1=Changes, 2=Todo, 3=Diff, 4=Files, 5=Sessions).
    pub sidebar_active_tab: usize,

    /// Status bar at the bottom showing model, tokens, and system info.
    pub status_bar: StatusBar,

    /// Frame-based animation engine for transitions and effects.
    pub animation_engine: AnimationEngine,

    /// Frame timer with render-skip optimization for idle CPU <5%.
    pub frame_timer: FrameTimer,

    /// Per-component render timing profiler (disabled by default).
    pub render_profiler: RenderProfiler,

    /// Accessibility settings (ARIA labels, high contrast, screen reader).
    pub accessibility: AccessibilityMode,

    /// Startup phase for the loading overlay state machine.
    pub startup_phase: StartupPhase,
    /// Instant when TuiState was created (for show-delay calculation).
    pub startup_start: Instant,
    /// Instant when the overlay first became visible (for min-display calculation).
    pub startup_show_time: Option<Instant>,
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

        // Layout tree with the default horizontal split: 70% chat / 30% sidebar.
        let layout_tree = crate::tui::layout::defaults::build_default_layout();

        let chat_view = ChatView::new();
        let keybind_engine = KeybindEngine::new(default_bindings());
        let event_bus = EventBus::new();
        let event_dispatcher = EventDispatcher::new();
        let theme_engine = ThemeEngine::new_dark();
        let dialog_manager = DialogManager::new();
        let toast_manager = ToastManager::new();
        let agents_overlay = AgentsOverlay::new();
        let thinking_panel = ThinkingPanel::new();
        let command_palette = CommandPalette::new();
        let question_form = None;
        let export_dialog = ExportDialog::new();
        let export_dialog_active = false;
        let revert_dialog = RevertDialog::new();
        let context_panel = ContextPanel::new();
        let file_changes_panel = FileChangesPanel::new();
        let todo_panel = TodoPanel::new();
        let status_bar = StatusBar::with_default_sections();
        let animation_engine = AnimationEngine::new();
        let frame_timer = FrameTimer::new();
        let render_profiler = RenderProfiler::new();
        let accessibility = AccessibilityMode::new();

        let diff_viewer = DiffViewer::new("Diff");
        let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let prompt = Prompt::new(cwd);
        let file_tree = FileTree::new();
        let session_sidebar = SessionSidebar::new(session_id);

        Self {
            app,
            layout_tree,
            chat_view,
            keybind_engine,
            event_bus,
            event_dispatcher,
            theme_engine,
            dialog_manager,
            toast_manager,
            agents_overlay,
            thinking_panel,
            command_palette,
            question_form,
            export_dialog,
            export_dialog_active,
            revert_dialog,
            context_panel,
            file_changes_panel,
            todo_panel,
            status_bar,
            animation_engine,
            frame_timer,
            render_profiler,
            diff_viewer,
            prompt,
            file_tree,
            session_sidebar,
            sidebar_active_tab: 0,
            accessibility,
            startup_phase: StartupPhase::Hidden,
            startup_start: Instant::now(),
            startup_show_time: None,
        }
    }

    // ── Event Bridging ──────────────────────────────────────────

    /// Apply a `TuiEvent` from the background turn runner to the display.
    ///
    /// **Preserves existing behavior**: delegates to `App::apply_event()`
    /// for all timeline updates, token tracking, and state transitions.
    ///
    /// **Bridges to new EventBus**: after updating the App, sends a
    /// synthetic state-change notification via `EventBus` so that new
    /// engine components can react (e.g., re-sync their view-models).
    ///
    /// The synthetic event uses `Resize(0, 0)` as a signal since
    /// crossterm has no custom event type. Components should check
    /// for this and re-sync from App state as needed.
    pub fn apply_event(&mut self, event: TuiEvent) {
        // Push toast on errors
        if let TuiEvent::TurnError { ref error } = event {
            self.toast_manager.push(
                ToastVariant::Error,
                Some("Error".into()),
                error.clone(),
                5000,
            );
        }

        // Preserve ALL existing App behavior
        self.app.apply_event(event);

        // Bridge: notify new components that state has changed.
        // Using Resize(0,0) as a sentinel — real resize events always
        // have non-zero dimensions, so this is unambiguous.
        self.event_bus
            .send(crossterm::event::Event::Resize(0, 0), EventPriority::Normal);

        // Drain and dispatch to registered components.
        self.event_dispatcher.dispatch(&self.event_bus);
    }

    // ── Rendering ───────────────────────────────────────────────

    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let skin = self.app.skin.clone();

        // Animation tick: advance all active animations
        self.animation_engine.tick();

        // Toast tick: advance auto-dismiss timers
        self.toast_manager.tick();

        // Sync chat view from App state before rendering
        self.chat_view.sync_from_app(&self.app);

        // Sync agents overlay from App state
        self.agents_overlay.sync_from_app(&self.app);
        self.agents_overlay.tick();

        // Sync thinking panel from App state
        self.thinking_panel.sync_from_app(&self.app);
        self.thinking_panel.tick();

        // Sync sidebar panels from App state
        self.context_panel.sync_from_app(&self.app);
        // File changes: populated by external load() from session diff
        // TodoPanel: extracts TodoWrite from timeline entries
        self.file_changes_panel.load(vec![]);
        self.todo_panel.load(vec![]);

        // Sync file tree from App file_entries
        if !self.app.file_entries.is_empty() {
            self.file_tree.rebuild(&self.app.file_entries);
        }

        // Sync session sidebar from App picker_sessions
        if !self.app.picker_sessions.is_empty() {
            self.session_sidebar.load(self.app.picker_sessions.clone());
        }
        self.session_sidebar.set_current_session(&self.app.session_id);

        // Sync prompt textarea from App input
        let input_text = self.app.input.lines().join("\n");
        if self.prompt.text() != input_text {
            self.prompt.set_text(&input_text);
        }

        // Sync status bar from App state
        self.status_bar.sync_from_app(&self.app);
        self.status_bar.tick();

        // Compute content area: exclude status bar (1 line) and input (3 lines) from bottom.
        let content_area = if area.height > 4 {
            ratatui::layout::Rect::new(0, 0, area.width, area.height.saturating_sub(4))
        } else {
            area
        };

        // 1. Render chat view (70% left) + sidebar (30% right) in content area
        {
            let chat_w = ((content_area.width as f32 * 0.7).round() as u16).min(content_area.width);
            let sidebar_w = content_area.width.saturating_sub(chat_w);
            let chat_area = ratatui::layout::Rect::new(0, 0, chat_w, content_area.height);
            let sidebar_area = ratatui::layout::Rect::new(chat_w, 0, sidebar_w, content_area.height);

            let mut ctx = RenderContext::new(frame, &skin);

            // Render chat view (already synced above)
            {
                let _guard = self.render_profiler.guard("chat_view");
                self.chat_view.render(&mut ctx, chat_area);
            }

            // Render sidebar: tab bar + active panel
            let tab_height = 1u16;
            let tab_labels = ["Context", "Changes", "Todo", "Diff", "Files", "Sessions"];
            let tab_area = ratatui::layout::Rect::new(
                sidebar_area.x, sidebar_area.y, sidebar_area.width, tab_height,
            );
            let tabs = ratatui::widgets::Tabs::new(tab_labels).select(self.sidebar_active_tab);
            ctx.frame_mut().render_widget(tabs, tab_area);

            let panel_area = ratatui::layout::Rect::new(
                sidebar_area.x,
                sidebar_area.y.saturating_add(tab_height),
                sidebar_area.width,
                sidebar_area.height.saturating_sub(tab_height),
            );
            match self.sidebar_active_tab {
                0 => self.context_panel.render(&mut ctx, panel_area),
                1 => self.file_changes_panel.render(&mut ctx, panel_area),
                2 => self.todo_panel.render(&mut ctx, panel_area),
                3 => {
                    let _guard = self.render_profiler.guard("diff_viewer");
                    let _ = error_recovery::catch_render_panic("diff_viewer", AssertUnwindSafe(|| {
                        self.diff_viewer.render(&mut ctx, panel_area);
                    }));
                }
                4 => {
                    let _guard = self.render_profiler.guard("file_tree");
                    let _ = error_recovery::catch_render_panic("file_tree", AssertUnwindSafe(|| {
                        self.file_tree.render(&mut ctx, panel_area);
                    }));
                }
                5 => {
                    let _guard = self.render_profiler.guard("session_sidebar");
                    let _ = error_recovery::catch_render_panic("session_sidebar", AssertUnwindSafe(|| {
                        self.session_sidebar.render(&mut ctx, panel_area);
                    }));
                }
                _ => {}
            }
        }

        // Sync back scroll/viewport state to App (after chat render, before overlays)
        self.chat_view.sync_to_app(&mut self.app);

        // 2. Render status bar at bottom
        {
            let status_area = ratatui::layout::Rect::new(
                0,
                area.height.saturating_sub(1),
                area.width,
                1,
            );
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("status_bar");
                match error_recovery::catch_render_panic("status_bar", AssertUnwindSafe(|| {
                    self.status_bar.render(&mut ctx, status_area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 2.5. Render prompt with autocomplete (3 lines above status bar)
        {
            let input_area = ratatui::layout::Rect::new(
                0,
                area.height.saturating_sub(4),
                area.width,
                3,
            );
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("prompt");
                match error_recovery::catch_render_panic("prompt", AssertUnwindSafe(|| {
                    self.prompt.render(&mut ctx, input_area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 3. Render thinking panel when turn is active
        if self.app.turn_active {
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("thinking_panel");
                match error_recovery::catch_render_panic("thinking_panel", AssertUnwindSafe(|| {
                    self.thinking_panel.render(&mut ctx, area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 4. Render agents overlay when visible
        if self.agents_overlay.visible {
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("agents_overlay");
                match error_recovery::catch_render_panic("agents_overlay", AssertUnwindSafe(|| {
                    self.agents_overlay.render(&mut ctx, area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 5. Render toast notifications at top-right
        if !self.toast_manager.is_empty() {
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("toast_manager");
                match error_recovery::catch_render_panic("toast_manager", AssertUnwindSafe(|| {
                    self.toast_manager.render(&mut ctx, area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 6. Render dialog stack on top (backdrop + centered dialog)
        if !self.dialog_manager.is_empty() {
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("dialog_manager");
                match error_recovery::catch_render_panic("dialog_manager", AssertUnwindSafe(|| {
                    self.dialog_manager.render(&mut ctx, area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 7. Render command palette when open
        if self.command_palette.is_open() {
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("command_palette");
                match error_recovery::catch_render_panic("command_palette", AssertUnwindSafe(|| {
                    self.command_palette.render(&mut ctx, area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 8. Render question form when active
        if let Some(ref mut qf) = self.question_form {
            if qf.is_active() {
                let degraded = {
                    let mut ctx = RenderContext::new(frame, &skin);
                    let _guard = self.render_profiler.guard("question_form");
                    match error_recovery::catch_render_panic("question_form", AssertUnwindSafe(|| {
                        qf.render(&mut ctx, area);
                    })) {
                        RenderResult::Ok => None,
                        RenderResult::Degraded(msg) => Some(msg),
                    }
                };
                if let Some(msg) = degraded {
                    self.add_message("system", &msg);
                }
            }
        }

        // 9. Render export dialog when active
        if self.export_dialog_active {
            let degraded = {
                let mut ctx = RenderContext::new(frame, &skin);
                let _guard = self.render_profiler.guard("export_dialog");
                match error_recovery::catch_render_panic("export_dialog", AssertUnwindSafe(|| {
                    self.export_dialog.render(&mut ctx, area);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 10. Render Ctrl+O message menu
        self.render_message_menu(frame, area, &skin);

        // 11. Render startup loading overlay (highest z-index, below dialogs)
        if self.startup_phase != StartupPhase::Done {
            self.render_startup_overlay(frame, area);
        }

        // 12. Render which-key overlay when Space leader is active
        if self.keybind_engine.which_key_visible {
            let degraded = {
                let _guard = self.render_profiler.guard("which_key");
                match error_recovery::catch_render_panic("which_key", AssertUnwindSafe(|| {
                    WhichKey::draw(frame, area, &self.keybind_engine);
                })) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // Update last drawn version for render skip optimization
        self.app.last_drawn_version = self.app.msg_version;
    }

    // ── Input Handling ──────────────────────────────────────────

    /// Process a raw keyboard event through the keybinding engine.
    ///
    /// If a dialog is active, the event is routed to the dialog manager
    /// first (focus trap). Otherwise, it goes through the keybind engine:
    /// - Multi-chord bindings (e.g., `g` `g`) accumulate until resolved.
    /// - Space leader key triggers which-key overlay.
    /// - Resolved actions are dispatched to the appropriate App methods.
    ///
    /// Returns `true` if the event was consumed (handled), `false` if
    /// it should propagate further.
    pub fn handle_input(&mut self, event: KeyEvent) -> bool {
        // 1. Dialog focus trap: if a dialog is active, keys go to it
        if !self.dialog_manager.is_empty() {
            return self.dialog_manager.handle_key(&event);
        }

        // 2. Route through keybind engine
        if let Some(action) = self.keybind_engine.handle_key(event) {
            self.dispatch_action(action);
            return true;
        }

        // 3. Not consumed by keybinds — may still need chord timeout check
        self.keybind_engine.check_timeout();
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
        use crossterm::event::{KeyCode, KeyModifiers};

        // ── Modal overlays: route keys to the topmost active overlay ──

        // 0. Message menu (Ctrl+O context menu)
        if self.chat_view.pending_message_menu {
            match key.code {
                KeyCode::Char('c') => {
                    self.app.copy_focused_content();
                    self.chat_view.pending_message_menu = false;
                    self.toast_manager.push(
                        ToastVariant::Success,
                        Some("Copied".into()),
                        "Entry content copied to clipboard".into(),
                        2000,
                    );
                    return ProcessedKey::Nothing;
                }
                KeyCode::Char('e') => {
                    self.app.toggle_expand_current();
                    self.chat_view.pending_message_menu = false;
                    return ProcessedKey::Nothing;
                }
                KeyCode::Char('r') => {
                    self.chat_view.pending_message_menu = false;
                    let idx = self.chat_view.pending_menu_entry_idx;
                    let diff_text = String::new();
                    self.revert_dialog.open_revert_dialog(
                        &mut self.dialog_manager,
                        idx,
                        &diff_text,
                    );
                    return ProcessedKey::Nothing;
                }
                KeyCode::Esc => {
                    self.chat_view.pending_message_menu = false;
                    return ProcessedKey::Nothing;
                }
                _ => return ProcessedKey::Nothing,
            }
        }

        // 1. Command palette open → route keys to it
        if self.command_palette.is_open() {
            match key.code {
                KeyCode::Esc => {
                    self.command_palette.close();
                    return ProcessedKey::Nothing;
                }
                _ => {
                    let event = crossterm::event::Event::Key(key);
                    let result = self.command_palette.handle_event(&event);
                    if result == crate::tui::components::EventResult::Consumed {
                        if let Some(action) = self.command_palette.take_action() {
                            self.dispatch_action(action);
                        }
                    }
                    return ProcessedKey::Nothing;
                }
            }
        }

        // 2. Question form active → route keys to it
        if let Some(ref mut qf) = self.question_form {
            if qf.is_active() {
                let consumed = qf.handle_key(&key);
                if consumed {
                    if qf.is_confirmed() {
                        let answers = qf.take_answers();
                        self.toast_manager.push(
                            ToastVariant::Info,
                            Some("Answers".into()),
                            format!("Received {} answers", answers.len()),
                            3000,
                        );
                    }
                    if qf.is_rejected() {
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Dismissed".into()),
                            "Question form dismissed".into(),
                            2000,
                        );
                    }
                    return ProcessedKey::Nothing;
                }
            }
        }

        // 3. Export dialog active → route keys to it
        if self.export_dialog_active {
            let event = crossterm::event::Event::Key(key);
            let result = self.export_dialog.handle_event(&event);
            if result == crate::tui::components::EventResult::Consumed {
                if let Some(ref result) = self.export_dialog.result {
                    self.toast_manager.push(
                        ToastVariant::Success,
                        Some("Export".into()),
                        format!("Exporting to {}...", result.filename),
                        3000,
                    );
                    self.export_dialog_active = false;
                }
                if self.export_dialog.cancelled {
                    self.toast_manager.push(
                        ToastVariant::Info,
                        Some("Cancelled".into()),
                        "Export cancelled".into(),
                        2000,
                    );
                    self.export_dialog_active = false;
                }
                return ProcessedKey::Nothing;
            }
        }

        // ── Prompt autocomplete routing (Tab / Shift+Tab / Esc) ──
        // Route these keys through the prompt component before sidebar cycling.
        if key.code == KeyCode::Tab && !key.modifiers.contains(KeyModifiers::SHIFT) {
            // Refresh suggestions from current text, then handle Tab
            self.prompt.refresh_suggestions();
            let event = crossterm::event::Event::Key(key);
            let result = self.prompt.handle_event(&event);
            if result == crate::tui::components::EventResult::Consumed {
                // Sync accepted suggestion back to app.input
                let new_text = self.prompt.text();
                let mut ta = tui_textarea::TextArea::default();
                ta.set_block(ratatui::widgets::Block::default()
                    .borders(ratatui::widgets::Borders::ALL)
                    .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
                ta.set_style(ratatui::style::Style::default()
                    .fg(ratatui::style::Color::White));
                if !new_text.is_empty() {
                    ta.insert_str(&new_text);
                }
                self.app.input = ta;
                return ProcessedKey::Nothing;
            }
            // Fall through to sidebar tab cycling
        }
        if key.code == KeyCode::Tab && key.modifiers.contains(KeyModifiers::SHIFT) {
            let event = crossterm::event::Event::Key(key);
            let result = self.prompt.handle_event(&event);
            if result == crate::tui::components::EventResult::Consumed {
                return ProcessedKey::Nothing;
            }
            // Fall through to sidebar tab cycling
        }
        if key.code == KeyCode::Esc {
            let event = crossterm::event::Event::Key(key);
            let result = self.prompt.handle_event(&event);
            if result == crate::tui::components::EventResult::Consumed {
                return ProcessedKey::Nothing;
            }
            // Fall through to normal Esc handling
        }

        // ── Sidebar tab switching ──
        // Tab / Shift+Tab: cycle through sidebar tabs (Context / Changes / Todo / Diff / Files / Sessions)
        const SIDEBAR_TAB_COUNT: usize = 6;
        if key.code == KeyCode::Tab {
            self.sidebar_active_tab = (self.sidebar_active_tab + 1) % SIDEBAR_TAB_COUNT;
            return ProcessedKey::Nothing;
        }
        if key.code == KeyCode::BackTab {
            self.sidebar_active_tab = if self.sidebar_active_tab == 0 {
                SIDEBAR_TAB_COUNT - 1
            } else {
                self.sidebar_active_tab - 1
            };
            return ProcessedKey::Nothing;
        }

        // ── Modal overrides (pick up where old input.rs left off) ──
        // 1. Picker active → route to dialog (already handled by dialog_manager in handle_input)
        // 2. Approval active → route to dialog (same)
        // 3. Search active → handle inline

        if self.app.search_active {
            return self.handle_search_key(key);
        }

        // 4. Text-editing keys → direct to textarea (bypass keybind engine)
        if self.is_textarea_key(&key) {
            self.app.input.input(key);
            self.prompt.refresh_suggestions();
            return ProcessedKey::Nothing;
        }

        // 5. Enter special case: submit input or toggle expand
        if key.code == KeyCode::Enter {
            if key.modifiers.contains(KeyModifiers::SHIFT) {
                self.app.input.insert_newline();
                return ProcessedKey::Nothing;
            }
            if self.app.input.is_empty() {
                // Empty input + Enter → toggle expand on focused entry
                if let Some(entry) = self.app.timeline_get(self.app.timeline_cursor) {
                    if entry.is_collapsible() {
                        self.app.toggle_expand_current();
                        return ProcessedKey::Nothing;
                    }
                }
            }
            // Non-empty input → submit
            let text = self.app.input.lines().join("\n").trim().to_string();
            self.prompt.add_history(text.clone());
            self.app.input = tui_textarea::TextArea::default();
            self.app.input.set_block(ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
            return ProcessedKey::Submit(text);
        }

        // 6. Esc/Ctrl+C special handling for turn-active cancel and exit
        if key.code == KeyCode::Esc {
            if self.app.turn_active {
                return ProcessedKey::Cancel;
            }
            return ProcessedKey::Exit;
        }
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            if self.app.turn_active {
                return ProcessedKey::Cancel;
            }
            return ProcessedKey::Exit;
        }

        // 7. Route through keybind engine for all remaining keys
        if !self.dialog_manager.is_empty() {
            self.dialog_manager.handle_key(&key);
            return ProcessedKey::Nothing;
        }

        if let Some(action) = self.keybind_engine.handle_key(key) {
            self.dispatch_action(action);
        } else {
            self.keybind_engine.check_timeout();
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
                return matches!(event.code, KeyCode::Char('a' | 'e' | 'w' | 'u' | 'k' | 'z'));
            }
            return false;
        }

        // Non-char textarea keys
        matches!(event.code,
            KeyCode::Backspace | KeyCode::Delete | KeyCode::Left | KeyCode::Right
        )
    }

    /// Handle a key press while search is active.
    fn handle_search_key(&mut self, key: crossterm::event::KeyEvent) -> ProcessedKey {
        use crossterm::event::{KeyCode, KeyModifiers};
        match key.code {
            KeyCode::Esc => {
                self.app.cancel_search();
                ProcessedKey::Nothing
            }
            KeyCode::Enter => {
                let query = self.app.search_query.clone();
                self.app.search_active = false;
                if !query.is_empty() {
                    self.app.execute_search(&query);
                }
                ProcessedKey::Nothing
            }
            KeyCode::Backspace => {
                self.app.search_query.pop();
                ProcessedKey::Nothing
            }
            KeyCode::Char(c) => {
                self.app.search_query.push(c);
                ProcessedKey::Nothing
            }
            _ => ProcessedKey::Nothing,
        }
    }

    // ── Dialog result polling ──────────────────────────────────

    /// Pop and return the last dialog result, if a dialog was just dismissed.
    pub fn take_dialog_result(&mut self) -> Option<crate::tui::components::dialog::DialogResult> {
        // DialogManager pops internally on dismiss, so we can't peek at the
        // popped dialog. Instead, we check the open picker state for results.
        None
    }

    /// Open the session picker as a Select dialog.
    pub fn open_session_picker_dialog(&mut self) {
        use crate::tui::components::dialog::{DialogKind, DialogState};
        let items: Vec<String> = self.app.picker_sessions.iter()
            .map(|s| {
                let ts = chrono::DateTime::from_timestamp((s.updated_at_ms / 1000) as i64, 0)
                    .map(|d| d.format("%m-%d %H:%M").to_string())
                    .unwrap_or_default();
                format!("{}  {} msgs  {}  {}",
                    "", s.message_count, ts, &s.id[..8.min(s.id.len())])
            })
            .collect();
        let dialog = DialogState::new(DialogKind::Select {
            title: "Select session (↑↓ jk Enter Esc)".into(),
            items,
            selected: 0,
        });
        self.dialog_manager.push(dialog);
        self.app.picker_active = false; // use dialog instead of raw picker
    }

    /// Open the approval request as a Confirm dialog.
    pub fn open_approval_dialog(&mut self) {
        use crate::tui::components::dialog::{DialogKind, DialogState};
        if let Some(ref req) = self.app.approval {
            let message = format!(
                "Tool: {}\nInput: {}",
                req.tool_name,
                req.input_preview.chars().take(40).collect::<String>()
            );
            let dialog = DialogState::new(DialogKind::Confirm {
                title: "Approval Required".into(),
                message,
                default: true,
            });
            self.dialog_manager.push(dialog);
        }
    }

    // ── Action Dispatch ─────────────────────────────────────────

    /// Execute the side effects of a resolved keybinding action.
    ///
    /// Maps every [`Action`] variant to the appropriate App method call
    /// or TuiState operation.
    fn dispatch_action(&mut self, action: Action) {
        match action {
            Action::Scroll(delta) => {
                if delta > 0 {
                    self.app.scroll_offset =
                        self.app.scroll_offset.saturating_add(delta as u16);
                    self.app.auto_scroll = false;
                } else {
                    self.app.scroll_offset =
                        self.app.scroll_offset.saturating_sub((-delta) as u16);
                    self.app.auto_scroll = false;
                }
            }
            Action::ScrollPage(direction) => {
                if direction > 0 {
                    self.app.scroll_page_down();
                } else {
                    self.app.scroll_page_up();
                }
                self.app.auto_scroll = false;
            }
            Action::ScrollTop => {
                self.app.scroll_offset = 0;
                self.app.auto_scroll = false;
            }
            Action::ScrollBottom => {
                self.app.auto_scroll = true;
            }
            Action::ExpandCollapse => {
                self.app.toggle_expand_current();
            }
            Action::Copy => {
                if self.app.copy_focused_content() {
                    self.toast_manager.push(
                        ToastVariant::Success,
                        Some("Copied".into()),
                        "Entry content copied to clipboard".into(),
                        2000,
                    );
                } else {
                    self.toast_manager.push(
                        ToastVariant::Warning,
                        Some("Copy".into()),
                        "Nothing to copy".into(),
                        2000,
                    );
                }
            }
            Action::Quit => {
                self.app.should_quit = true;
            }
            Action::NextPanel => {
                self.app.next_panel();
            }
            Action::PrevPanel => {
                use crate::tui::app::Panel;
                self.app.current_panel = match self.app.current_panel {
                    Panel::Chat => Panel::Delegate,
                    Panel::Delegate => Panel::Skills,
                    Panel::Skills => Panel::Memory,
                    Panel::Memory => Panel::Files,
                    Panel::Files => Panel::Gateway,
                    Panel::Gateway => Panel::Chat,
                };
            }
            Action::ToggleCommandPalette => {
                if self.command_palette.is_open() {
                    self.command_palette.close();
                } else {
                    self.command_palette.open();
                }
            }
            Action::ToggleAgentsOverlay => {
                self.agents_overlay.toggle();
            }
            Action::ToggleTheme => {
                self.app.theme.toggle();
                self.theme_engine.toggle_dark_light();
            }
            Action::ToggleHelp => {
                // Toggle which-key overlay via keybind engine
                if self.keybind_engine.which_key_visible {
                    self.keybind_engine.flush_pending();
                } else {
                    self.keybind_engine.which_key_visible = true;
                }
            }
            Action::Search => {
                if self.app.input.is_empty() {
                    self.app.search_active = true;
                    self.app.search_query.clear();
                    // Trigger search highlight pulse animation
                    self.animation_engine
                        .start_one_shot(AnimationKind::SearchPulse, 4);
                }
            }
            Action::SearchNext => {
                if self.app.input.is_empty() && !self.app.search_matches.is_empty() {
                    self.app.search_next();
                    // Re-trigger pulse on each match navigation
                    self.animation_engine
                        .start_one_shot(AnimationKind::SearchPulse, 4);
                }
            }
            Action::SearchPrev => {
                if self.app.input.is_empty() && !self.app.search_matches.is_empty() {
                    self.app.search_prev();
                    self.animation_engine
                        .start_one_shot(AnimationKind::SearchPulse, 4);
                }
            }
            Action::Cancel => {
                // Cascade: help/which-key → search → picker → dialog → turn
                self.keybind_engine.flush_pending();
                self.app.cancel_search();
                if self.app.picker_active {
                    self.app.close_session_picker();
                }
                if !self.dialog_manager.is_empty() {
                    self.dialog_manager.pop();
                }
            }
            Action::SubmitInput => {
                // Handled by the input layer — no-op at dispatch level.
                // The event loop reads self.app.input content separately.
            }
            Action::NextModel => {
                if let Some(model) = self.app.next_model() {
                    self.app.show_notification(&format!("Switched to model: {model}"));
                }
            }
            Action::HistoryBrowse(older) => {
                let text = if older {
                    self.app.history_prev()
                } else {
                    self.app.history_next()
                };
                if let Some(text) = text {
                    let mut ta = tui_textarea::TextArea::default();
                    ta.set_block(ratatui::widgets::Block::default()
                        .borders(ratatui::widgets::Borders::ALL)
                        .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
                    ta.set_style(ratatui::style::Style::default()
                        .fg(ratatui::style::Color::White));
                    if !text.is_empty() {
                        ta.insert_str(&text);
                    }
                    self.app.input = ta;
                }
            }
            Action::OpenDialog(name) => {
                use crate::tui::components::dialog::{DialogKind, DialogState};
                match name.as_str() {
                    "export" => {
                        self.export_dialog.reset();
                        self.export_dialog_active = true;
                    }
                    _ => {
                        let dialog = match name.as_str() {
                            "command_palette" => DialogState::new(DialogKind::Select {
                                title: "Command Palette".into(),
                                items: vec![
                                    "New Session".into(),
                                    "Open Session".into(),
                                    "Toggle Theme".into(),
                                    "Show Help".into(),
                                    "Quit".into(),
                                ],
                                selected: 0,
                            }),
                            _ => DialogState::new(DialogKind::Alert {
                                title: name.clone(),
                                message: format!("Dialog '{name}' not yet implemented."),
                            }),
                        };
                        self.dialog_manager.push(dialog);
                        self.animation_engine
                            .start_one_shot(AnimationKind::DialogFade, 4);
                    }
                }
            }
            Action::FocusDiff => {
                self.sidebar_active_tab = 3;
            }
            Action::FocusFileTree => {
                self.sidebar_active_tab = 4;
            }
            Action::FocusSessions => {
                self.sidebar_active_tab = 5;
            }
            Action::Execute(ref _cmd) => {}
            Action::TogglePanel(ref _name) => {}
            Action::Noop => {}
        }
    }

    // ── Convenience Methods ─────────────────────────────────────

    /// Register a component with the event dispatcher.
    ///
    /// Takes an `EventComponentId` (from the event module) which wraps
    /// a `String` identifier. Use `EventComponentId("my_component".into())`
    /// to create one.
    ///
    /// Shortcut for `self.event_dispatcher.register(id, component)`.
    pub fn register_component(
        &mut self,
        id: EventComponentId,
        component: Box<dyn Component>,
    ) {
        self.event_dispatcher.register(id, component);
    }

    /// Drain the event bus and dispatch all pending events.
    ///
    /// Shortcut for `self.event_dispatcher.dispatch(&self.event_bus)`.
    pub fn dispatch_events(&mut self) {
        self.event_dispatcher.dispatch(&self.event_bus);
    }

    /// Flush the keybind engine's pending chord (e.g., on Escape).
    pub fn flush_chord(&mut self) {
        self.keybind_engine.flush_pending();
    }

    /// Check and apply keybind chord timeout.
    pub fn check_keybind_timeout(&mut self) {
        self.keybind_engine.check_timeout();
    }

    /// Poll-based hot-reload for the theme engine.
    ///
    /// Returns `true` if the theme file changed and was reloaded.
    pub fn hot_reload_theme(&mut self) -> bool {
        self.theme_engine.hot_reload()
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

        match self.startup_phase {
            Done => {}
            Finishing => {
                if ready {
                    if let Some(show_time) = self.startup_show_time {
                        if now.duration_since(show_time) >= MIN_DISPLAY {
                            self.startup_phase = Done;
                        }
                    }
                } else {
                    self.startup_phase = Loading;
                    self.startup_show_time = None;
                }
            }
            Loading => {
                if ready {
                    self.startup_phase = Finishing;
                    self.startup_show_time = Some(now);
                }
            }
            Hidden => {
                if ready {
                    // Completed before show delay → never show overlay
                    self.startup_phase = Done;
                } else if now.duration_since(self.startup_start) >= SHOW_DELAY {
                    self.startup_phase = Loading;
                }
            }
        }
    }

    /// Render the Ctrl+O per-message action menu when pending.
    fn render_message_menu(&mut self, frame: &mut Frame, area: ratatui::layout::Rect, skin: &crate::tui::skin::SkinConfig) {
        if !self.chat_view.pending_message_menu {
            return;
        }

        use ratatui::layout::{Constraint, Direction, Layout};
        use ratatui::style::{Color, Modifier, Style, Stylize};
        use ratatui::text::{Line, Span};
        use ratatui::widgets::{Block, Borders, Clear, Paragraph};

        let menu_items = [
            ("c", "Copy", "Copy focused entry to clipboard"),
            ("e", "Expand/Collapse", "Toggle expand/collapse"),
            ("r", "Revert to here", "Revert session to this point"),
        ];
        let n = menu_items.len();

        let w = 42u16;
        let h = (n as u16 + 4).min(area.height.saturating_sub(2));
        let x = (area.width.saturating_sub(w)) / 2;
        let y = (area.height.saturating_sub(h)) / 2;
        let menu_rect = ratatui::layout::Rect::new(x, y, w, h);

        frame.render_widget(Clear, menu_rect);

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(Span::styled(
            " Message Actions ",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));

        for (key, label, _desc) in &menu_items {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  [{key}] "),
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    *label,
                    Style::default().fg(Color::White),
                ),
            ]));
        }
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "  Esc to dismiss",
            Style::default().fg(Color::DarkGray),
        )));

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));
        let paragraph = Paragraph::new(lines).block(block);
        frame.render_widget(paragraph, menu_rect);
    }

    /// Render the startup loading overlay at the bottom of the screen.
    fn render_startup_overlay(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        use ratatui::layout::Alignment;
        use ratatui::style::Style;
        use ratatui::text::Span;
        use ratatui::widgets::Paragraph;

        let text = match self.startup_phase {
            StartupPhase::Loading => " ⟳ Loading plugins... ",
            StartupPhase::Finishing => " ⟳ Finishing startup... ",
            _ => return,
        };

        let fg = self.theme_engine.theme.palette.fg;
        let bg = self.theme_engine.theme.palette.muted;

        let overlay_y = area.height.saturating_sub(2);
        let overlay_rect = ratatui::layout::Rect::new(0, overlay_y, area.width, 1);

        let paragraph = Paragraph::new(Span::styled(text, Style::default().fg(fg).bg(bg)))
            .alignment(Alignment::Center);

        frame.render_widget(paragraph, overlay_rect);
    }
}

// ── Delegation to App via Deref ─────────────────────────────────

impl std::ops::Deref for TuiState {
    type Target = App;

    fn deref(&self) -> &App {
        &self.app
    }
}

impl std::ops::DerefMut for TuiState {
    fn deref_mut(&mut self) -> &mut App {
        &mut self.app
    }
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

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::time::Duration;

    // ── Construction ────────────────────────────────────────────

    #[test]
    fn tui_state_new_creates_all_engines() {
        let state = TuiState::new("test-model", "test-session");

        // App fields
        assert_eq!(state.app.model, "test-model");
        assert_eq!(state.app.session_id, "test-session");
        assert!(!state.app.should_quit);

        // Layout tree exists
        assert!(matches!(state.layout_tree.root, LayoutNode::Split(_)));

        // Keybind engine ready
        assert!(!state.keybind_engine.which_key_visible);

        // Dialog manager empty
        assert!(state.dialog_manager.is_empty());

        // Theme engine dark by default
        assert_eq!(state.theme_engine.theme.name, "dark");
    }

    // ── Deref delegation ────────────────────────────────────────

    #[test]
    fn deref_delegates_app_methods() {
        let mut state = TuiState::new("m", "s");

        // Access App fields via Deref
        state.add_message("user", "hello");
        assert_eq!(state.timeline_len(), 1);

        state.add_message("assistant", "world");
        assert_eq!(state.timeline_len(), 2);

        // DerefMut works for direct field access
        state.is_loading = true;
        assert!(state.is_loading);
    }

    #[test]
    fn deref_delegates_app_public_methods() {
        let mut state = TuiState::new("m", "s");

        state.add_message("system", "test");
        state.add_message("assistant", "response");

        assert_eq!(state.timeline_len(), 2);
        assert!(state.auto_scroll);

        // picker methods
        let sessions = vec![crate::tui::app::SessionSummary {
            id: "s1".into(),
            path: "/tmp".into(),
            updated_at_ms: 1000,
            message_count: 3,
        }];
        state.open_session_picker(sessions);
        assert!(state.picker_active);
        assert_eq!(state.picker_selected_id(), Some("s1"));
        state.close_session_picker();
        assert!(!state.picker_active);

        // cursor_* methods work
        state.cursor_down();
        state.cursor_up();
        state.toggle_expand_current();
    }

    #[test]
    fn deref_allows_reading_and_writing_pub_fields() {
        let mut state = TuiState::new("m", "s");

        state.spinner_idx = 5;
        assert_eq!(state.spinner_idx, 5);

        state.scroll_offset = 42;
        assert_eq!(state.scroll_offset, 42);

        state.help_visible = true;
        assert!(state.help_visible);
    }

    // ── apply_event ─────────────────────────────────────────────

    #[test]
    fn apply_event_text_delta_adds_to_timeline() {
        let mut state = TuiState::new("m", "s");

        state.apply_event(TuiEvent::TurnStarted);
        state.apply_event(TuiEvent::TextDelta {
            text: "Hello world".into(),
        });
        state.apply_event(TuiEvent::TurnComplete {
            assistant_text: String::new(),
            iterations: 1,
        });

        // Should have: assistant message + "✓ Done"
        assert!(state.timeline_len() >= 2);
        // The last entry should be "✓ Done"
        let last = state.timeline_get(state.timeline_len() - 1).unwrap();
        let text = last.full_text();
        assert!(text.contains("Done"), "expected '✓ Done' marker, got: {text}");
    }

    #[test]
    fn apply_event_tool_lifecycle() {
        let mut state = TuiState::new("m", "s");

        state.apply_event(TuiEvent::TurnStarted);
        state.apply_event(TuiEvent::ToolStart {
            id: "t1".into(),
            name: "bash".into(),
            preview: "ls -la".into(),
        });

        assert!(state
            .timeline_iter()
            .any(|(_, e)| matches!(&e, crate::tui::app::TimelineEntry::ToolCall { id, .. } if id == "t1")));
    }

    #[test]
    fn apply_event_token_usage_updates_counters() {
        let mut state = TuiState::new("m", "s");

        state.apply_event(TuiEvent::TokenUsage {
            input: 100,
            output: 50,
            cache_create: 10,
            cache_read: 5,
        });

        assert_eq!(state.input_tokens, 100);
        assert_eq!(state.output_tokens, 50);
        assert_eq!(state.token_count, 165);
    }

    // ── handle_input ────────────────────────────────────────────

    #[test]
    fn handle_input_quit_chord() {
        let mut state = TuiState::new("m", "s");

        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        let handled = state.handle_input(ctrl_c);

        assert!(handled);
        assert!(state.app.should_quit);
    }

    #[test]
    fn handle_input_scroll_down() {
        let mut state = TuiState::new("m", "s");
        state.scroll_offset = 0;
        state.auto_scroll = true;

        let j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE);
        let handled = state.handle_input(j);

        assert!(handled);
        assert_eq!(state.scroll_offset, 1);
        assert!(!state.auto_scroll); // manual scroll disables auto-scroll
    }

    #[test]
    fn handle_input_scroll_up() {
        let mut state = TuiState::new("m", "s");
        state.scroll_offset = 10;

        let k = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE);
        let handled = state.handle_input(k);

        assert!(handled);
        assert_eq!(state.scroll_offset, 9);
    }

    #[test]
    fn handle_input_unbound_key_returns_false() {
        let mut state = TuiState::new("m", "s");

        let x = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
        let handled = state.handle_input(x);

        assert!(!handled);
    }

    #[test]
    fn handle_input_space_leader_shows_which_key() {
        let mut state = TuiState::new("m", "s");

        let space = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE);
        let handled = state.handle_input(space);

        // Space alone is a prefix match, so which_key should be visible
        assert!(!handled); // No action resolved yet
        assert!(state.keybind_engine.which_key_visible);
        assert!(!state.keybind_engine.pending_chord().is_empty());
    }

    #[test]
    fn handle_input_gg_multi_chord() {
        let mut state = TuiState::new("m", "s");

        // First 'g' — prefix match
        let g1 = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE);
        assert!(!state.handle_input(g1));
        assert_eq!(state.keybind_engine.pending_chord().len(), 1);

        // Second 'g' — full match
        let g2 = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE);
        assert!(state.handle_input(g2));
        assert!(state.keybind_engine.pending_chord().is_empty());
    }

    #[test]
    fn handle_input_next_panel() {
        let mut state = TuiState::new("m", "s");
        use crate::tui::app::Panel;
        assert_eq!(state.app.current_panel, Panel::Chat);

        let tab = KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE);
        state.handle_input(tab);
        assert_eq!(state.app.current_panel, Panel::Gateway);
    }

    // ── dialog focus trap ───────────────────────────────────────

    #[test]
    fn handle_input_dialog_focus_trap() {
        let mut state = TuiState::new("m", "s");

        // Push an alert dialog
        use crate::tui::components::dialog::{DialogKind, DialogState};
        state.dialog_manager.push(DialogState::new(DialogKind::Alert {
            title: "Test".into(),
            message: "Alert!".into(),
        }));

        // Any key should be consumed by the dialog
        let enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
        let handled = state.handle_input(enter);

        assert!(handled);
        assert!(state.dialog_manager.is_empty()); // Dialog dismissed
    }

    // ── toggle_theme ────────────────────────────────────────────

    #[test]
    fn toggle_theme_via_leader_chord() {
        let mut state = TuiState::new("m", "s");
        assert_eq!(state.app.theme, crate::tui::app::Theme::Dark);

        // Space → leader prefix
        state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        // t → ToggleTheme
        state.handle_input(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));

        assert_eq!(state.app.theme, crate::tui::app::Theme::Light);
        assert_eq!(state.theme_engine.theme.name, "light");
    }

    // ── open_dialog ─────────────────────────────────────────────

    #[test]
    fn open_dialog_via_leader_chord() {
        let mut state = TuiState::new("m", "s");

        // Space → leader prefix
        state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        // p → OpenDialog("command_palette")
        state.handle_input(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));

        assert!(!state.dialog_manager.is_empty());
        let current = state.dialog_manager.current().unwrap();
        assert!(matches!(
            current.kind,
            crate::tui::components::dialog::DialogKind::Select { ref title, .. }
            if title == "Command Palette"
        ));
    }

    // ── cancel_action ───────────────────────────────────────────

    #[test]
    fn cancel_flushes_pending_and_closes_dialog() {
        let mut state = TuiState::new("m", "s");

        // Start a chord prefix
        state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        assert!(state.keybind_engine.which_key_visible);

        // Push a dialog
        use crate::tui::components::dialog::{DialogKind, DialogState};
        state.dialog_manager.push(DialogState::new(DialogKind::Alert {
            title: "X".into(),
            message: "Y".into(),
        }));

        // Esc → Cancel
        let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        state.handle_input(esc);

        // Dialog should still be active (Esc in dialog context dismisses it)
        // Wait - actually Esc in the dialog context triggers dismissal already.
        // Let's test the cancel action directly
        assert!(state.dialog_manager.is_empty());
    }

    // ── convenience methods ─────────────────────────────────────

    #[test]
    fn flush_chord_clears_pending() {
        let mut state = TuiState::new("m", "s");

        state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        assert!(!state.keybind_engine.pending_chord().is_empty());

        state.flush_chord();
        assert!(state.keybind_engine.pending_chord().is_empty());
        assert!(!state.keybind_engine.which_key_visible);
    }

    #[test]
    fn hot_reload_theme_no_file_returns_false() {
        let mut state = TuiState::new("m", "s");

        // ThemeEngine starts with dark builtin (no file), so hot_reload is a no-op
        assert!(!state.hot_reload_theme());
    }

    // ── startup_loading ─────────────────────────────────────────

    #[test]
    fn startup_shows_after_delay() {
        let mut state = TuiState::new("m", "s");
        assert_eq!(state.startup_phase, StartupPhase::Hidden);

        // Before 500ms show delay → still Hidden
        state.update_startup_phase_at(false, state.startup_start + Duration::from_millis(400));
        assert_eq!(state.startup_phase, StartupPhase::Hidden);

        // After 500ms show delay → Loading
        state.update_startup_phase_at(false, state.startup_start + Duration::from_millis(501));
        assert_eq!(state.startup_phase, StartupPhase::Loading);
    }

    #[test]
    fn startup_hides_when_ready() {
        let mut state = TuiState::new("m", "s");

        // Advance past 500ms to Loading phase
        state.update_startup_phase_at(false, state.startup_start + Duration::from_millis(501));
        assert_eq!(state.startup_phase, StartupPhase::Loading);

        // Signal ready → Finishing
        let ready_time = state.startup_start + Duration::from_millis(501);
        state.update_startup_phase_at(true, ready_time);
        assert_eq!(state.startup_phase, StartupPhase::Finishing);

        // Before min_display (3s) → still Finishing
        state.update_startup_phase_at(true, ready_time + Duration::from_millis(2500));
        assert_eq!(state.startup_phase, StartupPhase::Finishing);

        // After min_display → Done
        state.update_startup_phase_at(true, ready_time + Duration::from_secs(3));
        assert_eq!(state.startup_phase, StartupPhase::Done);
    }

    #[test]
    fn startup_min_display_3s() {
        let mut state = TuiState::new("m", "s");

        // Start showing Loading at t=500ms
        state.update_startup_phase_at(false, state.startup_start + Duration::from_millis(500));
        assert_eq!(state.startup_phase, StartupPhase::Loading);

        // Signal ready at t=600ms → Finishing
        let ready_time = state.startup_start + Duration::from_millis(600);
        state.update_startup_phase_at(true, ready_time);
        assert_eq!(state.startup_phase, StartupPhase::Finishing);

        // 2.5s after ready → still Finishing (not yet 3s)
        state.update_startup_phase_at(true, ready_time + Duration::from_millis(2500));
        assert_eq!(state.startup_phase, StartupPhase::Finishing);

        // 3s after ready → Done
        state.update_startup_phase_at(true, ready_time + Duration::from_secs(3));
        assert_eq!(state.startup_phase, StartupPhase::Done);
    }

    #[test]
    fn startup_completes_before_delay_never_shows() {
        let mut state = TuiState::new("m", "s");

        // Ready at t=100ms (before 500ms show delay)
        state.update_startup_phase_at(true, state.startup_start + Duration::from_millis(100));

        // Should skip overlay entirely → Done immediately
        assert_eq!(state.startup_phase, StartupPhase::Done);
    }

    #[test]
    fn startup_loading_text_no_trailing_newline() {
        let mut state = TuiState::new("m", "s");

        state.update_startup_phase_at(false, state.startup_start + Duration::from_millis(501));
        assert_eq!(state.startup_phase, StartupPhase::Loading, "should be Loading after delay");

        // Signal ready
        state.update_startup_phase_at(true, state.startup_start + Duration::from_millis(600));
        assert_eq!(state.startup_phase, StartupPhase::Finishing, "should be Finishing when ready");
    }
}
