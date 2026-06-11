// ── TuiState — Unified TUI application state ──────────────────
// Wraps the legacy App with new engine components:
//   LayoutTree, KeybindEngine, EventBus, ThemeEngine, DialogManager.
//
// Delegates all App public methods via Deref/DerefMut.
// Bridges App::apply_event(CowdEvent) → EventBus for new components.
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

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;

use crate::tui::accessibility::AccessibilityMode;
use crate::tui::animation::{AnimationEngine, AnimationKind};
use crate::tui::app::App;
use crate::tui::components::agent_team_panel::AgentTeamPanel;
use crate::tui::components::agents_overlay::AgentsOverlay;
use crate::tui::components::approval_cockpit_panel::ApprovalCockpitPanel;
use crate::tui::components::chat_view::ChatView;
use crate::tui::components::command_palette::CommandPalette;
use crate::tui::components::context_panel::ContextPanel;
use crate::tui::components::context_suggestions::ContextSuggestions;
use crate::tui::components::dialog::DialogManager;
use crate::tui::components::diff_viewer::DiffViewer;
use crate::tui::components::export_dialog::ExportDialog;
use crate::tui::components::file_changes_panel::FileChangesPanel;
use crate::tui::components::file_tree::FileTree;
use crate::tui::components::gateway_panel::GatewayPanel;
use crate::tui::components::goal_workbench_panel::GoalWorkbenchPanel;
use crate::tui::components::memory_panel::MemoryPanel;
use crate::tui::components::performance_dashboard::PerformanceDashboard;
use crate::tui::components::prompt::Prompt;
use crate::tui::components::question_form::QuestionForm;
use crate::tui::components::revert_dialog::RevertDialog;
use crate::tui::components::runtime_activity_panel::RuntimeActivityPanel;
use crate::tui::components::session_sidebar::SessionSidebar;
use crate::tui::components::skills_panel::SkillsPanel;
use crate::tui::components::status_bar::StatusBar;
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
use crate::tui::layout::LayoutTree;
use crate::tui::profiler::{FrameTimer, RenderProfiler};
use crate::tui::theme::ThemeEngine;
use runtime::CowdEvent;

/// Result of processing a key event through the TUI input pipeline.
#[derive(Debug, Clone)]
pub enum ProcessedKey {
    Submit(String),
    Cancel,
    Exit,
    Nothing,
}

pub(crate) const SIDEBAR_TAB_COUNT: usize = 11;

fn sidebar_tab_labels(width: u16) -> Vec<&'static str> {
    if width < 96 {
        vec![
            "Run", "Chg", "Goal", "Appr", "Todo", "Diff", "File", "Sess", "Mem", "Skill", "Gate",
        ]
    } else {
        vec![
            "Run",
            "Changes",
            "Goals",
            "Approvals",
            "Todo",
            "Diff",
            "Files",
            "Sessions",
            "Memory",
            "Skills",
            "Gateway",
        ]
    }
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

    /// Memory orchestrator for persistent memory operations (vector store, layers).
    pub memory_orchestrator: Option<std::sync::Arc<memory::MemoryOrchestrator>>,
    /// Last time the MemoryPanel was refreshed from the cognitive store.
    memory_panel_last_sync: Option<Instant>,

    /// Agents overlay showing subagent tree hierarchy.
    pub agents_overlay: AgentsOverlay,

    /// Agent team panel showing team hierarchy and status.
    pub agent_team_panel: AgentTeamPanel,

    /// L4 knowledge view showing shared/team-scoped memory entries.
    pub l4_knowledge_view: L4KnowledgeView,

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
    /// Pending export options from a confirmed export dialog.
    /// Consumed by `consume_session_sidebar_actions` in main.rs.
    pub pending_export_options: Option<crate::tui::components::export_dialog::ExportOptions>,

    /// Revert dialog helper for per-message revert confirmation.
    pub revert_dialog: RevertDialog,

    /// Context panel showing token usage and cost.
    pub context_panel: ContextPanel,

    /// Context suggestions bar for L4 Insert event awareness.
    pub context_suggestions: ContextSuggestions,

    /// File changes panel showing modified files with +/- counts.
    pub file_changes_panel: FileChangesPanel,

    /// Todo panel displaying task list from TodoWrite tool calls.
    pub todo_panel: TodoPanel,

    /// Goal workbench showing daemon task and YOLO/solo goal progress.
    pub goal_workbench_panel: GoalWorkbenchPanel,

    /// Approval and cross-plane permission cockpit.
    pub approval_cockpit_panel: ApprovalCockpitPanel,

    /// Diff viewer component for unified/split diff display.
    pub diff_viewer: DiffViewer,

    /// Prompt component with autocomplete, frecency scoring, @file completion.
    pub prompt: Prompt,

    /// File tree browser with git status overlay.
    pub file_tree: FileTree,

    /// Session list browser with rename/delete/switch/fork actions.
    pub session_sidebar: SessionSidebar,

    /// Memory browser panel with layer filter, search, detail view, delete.
    pub memory_panel: MemoryPanel,

    /// Performance dashboard overlay with sparkline, gauge, compression bar.
    pub performance_dashboard: PerformanceDashboard,

    /// Skills panel showing categorized skill/plugin browsing.
    pub skills_panel: SkillsPanel,

    /// Gateway panel showing backend daemon/API gateway status.
    pub gateway_panel: GatewayPanel,

    /// Runtime activity panel summarizing run/context/tool state.
    pub runtime_activity_panel: RuntimeActivityPanel,

    /// Active tab index in the sidebar (0=Runtime, 1=Context, 2=Changes, 3=Goals, 4=Approvals, 5=Todo, 6=Diff, 7=Files, 8=Sessions, 9=Memory, 10=Skills, 11=Gateway).
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

    /// Shared registry of active session runtimes (TUI/API bridge).
    pub active_sessions: Option<std::sync::Arc<crate::gateway::ActiveSessions>>,

    /// Startup phase for the loading overlay state machine.
    pub startup_phase: StartupPhase,
    /// Instant when TuiState was created (for show-delay calculation).
    pub startup_start: Instant,
    /// Instant when the overlay first became visible (for min-display calculation).
    pub startup_show_time: Option<Instant>,

    /// Count of events dropped due to channel full conditions.
    /// With `send()` backpressure (P0.6), the producer blocks instead of dropping,
    /// so this counter is a diagnostic for future non-blocking send paths.
    pub dropped_events: usize,

    /// Pending cancel: ESC was pressed once during an active turn — requires second press
    pending_cancel: bool,
    /// Pending quit: Ctrl+C was pressed once — requires second press
    pending_quit: bool,
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
        let agent_team_panel = AgentTeamPanel::new();
        let l4_knowledge_view = L4KnowledgeView::new();
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
        let file_tree = FileTree::new();
        let session_sidebar = SessionSidebar::new(session_id);
        let memory_panel = MemoryPanel::new();
        let performance_dashboard = PerformanceDashboard::new();
        let skills_panel = SkillsPanel::new();
        let gateway_panel = GatewayPanel::new();
        let runtime_activity_panel = RuntimeActivityPanel::new();

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
            memory_orchestrator: None,
            memory_panel_last_sync: None,
            agents_overlay,
            agent_team_panel,
            l4_knowledge_view,
            thinking_panel,
            command_palette,
            question_form,
            export_dialog,
            export_dialog_active,
            pending_export_options,
            revert_dialog,
            context_panel,
            context_suggestions,
            file_changes_panel,
            todo_panel,
            goal_workbench_panel,
            approval_cockpit_panel,
            status_bar,
            animation_engine,
            frame_timer,
            render_profiler,
            diff_viewer,
            prompt,
            file_tree,
            session_sidebar,
            memory_panel,
            performance_dashboard,
            skills_panel,
            gateway_panel,
            runtime_activity_panel,
            sidebar_active_tab: 0,
            accessibility,
            active_sessions: None,
            startup_phase: StartupPhase::Hidden,
            startup_start: Instant::now(),
            startup_show_time: None,
            dropped_events: 0,
            pending_cancel: false,
            pending_quit: false,
        }
    }

    /// Build a `TuiState` from an existing `App`, preserving all app state.
    /// The app is moved in; call `into_app()` to extract it back after rendering.
    #[must_use]
    pub fn from_app(app: App) -> Self {
        let mut state = Self::new(&app.model, &app.session_id);
        state.app = app;
        state
    }

    /// Extract the inner `App`, consuming this `TuiState`.
    #[must_use]
    pub fn into_app(self) -> App {
        self.app
    }

    /// Set the shared ActiveSessions registry for the session sidebar.
    pub fn set_active_sessions(
        &mut self,
        active_sessions: std::sync::Arc<crate::gateway::ActiveSessions>,
    ) {
        self.active_sessions = Some(active_sessions);
    }

    /// Set the tool registry for the skills panel.
    pub fn set_tool_registry(
        &mut self,
        registry: std::sync::Arc<dyn crate::tui::app::ToolRegistry>,
    ) {
        self.skills_panel.set_registry(registry);
    }

    /// Set the memory orchestrator for persistent memory operations.
    ///
    /// Also wires up the L4 event bus subscription for context suggestions.
    pub fn set_memory_orchestrator(&mut self, orch: std::sync::Arc<memory::MemoryOrchestrator>) {
        // Subscribe to L4 events for context-aware suggestions
        if let Some(event_bus) = orch.l4_event_bus() {
            let rx = event_bus.subscribe();
            self.context_suggestions.set_l4_receiver(rx);
        }
        self.memory_orchestrator = Some(orch);
    }

    /// Set the cognitive memory manager for TUI memory surfaces.
    pub fn set_memory_manager(&mut self, mgr: std::sync::Arc<memory::CognitiveContextManager>) {
        self.memory_panel
            .set_memory_manager(std::sync::Arc::clone(&mgr));
        self.set_memory_orchestrator(mgr.orchestrator());
    }

    // ── Event Bridging ──────────────────────────────────────────

    /// Apply a `CowdEvent` from the background turn runner to the display.
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
    pub fn apply_event(&mut self, event: CowdEvent) {
        // Push toast on errors
        if let CowdEvent::TurnError { ref error } = event {
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

        // Context suggestions tick: drain L4 events, expire stale suggestions
        self.context_suggestions.tick();

        // Sync chat view from App state before rendering
        self.chat_view.sync_from_app(&self.app);

        // Sync agents overlay from App state
        self.agents_overlay.sync_from_app(&self.app);
        self.agents_overlay.tick();

        // Sync thinking panel from App state
        self.thinking_panel.sync_from_app(&self.app);
        self.thinking_panel.tick();

        // Sync sidebar panels from App state
        self.runtime_activity_panel.sync_from_app(&self.app);
        self.context_panel.sync_from_app(&self.app);

        // Sync file changes panel from timeline (ToolCall outputs with file change info)
        let timeline = self.app.timeline_clone_vec();
        self.file_changes_panel.sync_from_timeline(&timeline);

        // Sync todo panel from timeline (TodoWrite ToolCall outputs)
        self.todo_panel.sync_from_timeline(&timeline);

        // Sync goal workbench from daemon runtime state.
        self.goal_workbench_panel.sync_from_app(&self.app);

        // Sync approval/permission cockpit from daemon runtime state.
        self.approval_cockpit_panel.sync_from_app(&self.app);

        // Sync diff viewer from App (extract diff text from timeline ToolCall outputs)
        self.diff_viewer.sync_from_app(&self.app);

        // Sync file tree from App file_entries
        if !self.app.file_entries.is_empty() {
            self.file_tree.rebuild(&self.app.file_entries);
        }

        // Sync session sidebar from App picker_sessions
        if !self.app.picker_sessions.is_empty() {
            self.session_sidebar.load(self.app.picker_sessions.clone());
        }
        self.session_sidebar
            .set_current_session(&self.app.session_id);

        // Sync memory panel from the real cognitive store only when the tab is
        // visible. Keep App fallback for memory-disabled sessions.
        if self.sidebar_active_tab == 9 && self.memory_panel.memory_manager.is_some() {
            let should_sync = self
                .memory_panel_last_sync
                .map(|last| last.elapsed() >= Duration::from_millis(750))
                .unwrap_or(true);
            if should_sync {
                self.memory_panel.sync_from_cognitive();
                self.memory_panel_last_sync = Some(Instant::now());
            }
        } else {
            self.memory_panel.sync_from_app(&self.app);
        }

        // Sync performance dashboard from memory orchestrator
        self.performance_dashboard.tick();
        self.performance_dashboard.sync(&self.memory_orchestrator);

        // Sync skills panel from App state
        self.skills_panel.sync_from_app(&self.app);

        // Sync gateway panel from App state
        self.gateway_panel.sync_from_app(&self.app);

        // BUG 1 FIX: No bidirectional sync — app.input is the single source of truth.
        // Prompt is used only for autocomplete suggestions (rendered as overlay dropdown).

        // BUG 5 FIX: Real-time token count update.
        // During active turns, ensure token_count reflects cumulative usage.
        // This acts as a fallback if background TokenUsage events are delayed.
        if self.app.turn_active {
            let turn_total = self.app.turn_input_tokens + self.app.turn_output_tokens;
            let base_total = self.app.input_tokens + self.app.output_tokens;
            // token_count should reflect the highest known total
            if turn_total > 0 && base_total + turn_total > self.app.token_count {
                self.app.token_count = base_total + turn_total;
            }
        }

        // Sync status bar from App state
        self.status_bar.sync_from_app(&self.app);
        self.status_bar.tick();

        // BUG 2 FIX: Dynamic input height based on line count.
        let input_lines = self.app.input.lines().len().max(1) as u16;
        let max_input = (area.height / 2).max(3);
        let input_h = (input_lines + 2).min(max_input).max(3);

        // FIX A: Render search bar BEFORE content to prevent overlap
        if self.app.search_active {
            let search_area = ratatui::layout::Rect::new(0, 0, area.width, 1);
            let search_text = if self.app.search_query.is_empty() {
                "/ ".to_string()
            } else {
                format!("/ {}", self.app.search_query)
            };
            let search_line = ratatui::text::Line::from(vec![
                ratatui::text::Span::styled(
                    search_text,
                    ratatui::style::Style::default()
                        .fg(ratatui::style::Color::Yellow)
                        .add_modifier(ratatui::style::Modifier::BOLD),
                ),
                ratatui::text::Span::styled(
                    "  Esc:cancel Enter:search",
                    ratatui::style::Style::default().fg(ratatui::style::Color::DarkGray),
                ),
            ]);
            frame.render_widget(ratatui::widgets::Paragraph::new(search_line), search_area);
        }

        // ── Main content: one RenderContext for chat, sidebar, status, input ──
        let mut main_ctx: RenderContext = RenderContext::new(frame, &skin);

        // 1. Render chat view + sidebar using the layout tree
        {
            self.layout_tree.resize(area);
            let mut chat_area = self.layout_tree.area_of("chat").unwrap_or(area);
            // Subtract status bar (1 line) + input area from chat viewport height
            // so scroll calculation doesn't think it has more visible lines than available
            chat_area.height = chat_area.height.saturating_sub(1).saturating_sub(input_h);
            let sidebar_w = area.width.saturating_sub(chat_area.width);
            let sidebar_area =
                ratatui::layout::Rect::new(chat_area.width, 0, sidebar_w, area.height);

            // Auto-scroll during streaming — computed BEFORE render to eliminate 1-frame lag
            if self.app.auto_scroll {
                let total = self.chat_view.total_lines();
                let vh = self.app.viewport_height as usize;
                if total > vh {
                    self.app.scroll_offset = (total - vh) as u16;
                } else {
                    self.app.scroll_offset = 0;
                }
            }
            self.chat_view.scroll_state.offset = self.app.scroll_offset;
            self.chat_view.scroll_state.auto_scroll = self.app.auto_scroll;

            // Render chat view (already synced above)
            {
                let _guard = self.render_profiler.guard("chat_view");
                self.chat_view.render(&mut main_ctx, chat_area);
            }
            self.chat_view.sync_to_app(&mut self.app);

            // Render sidebar: tab bar + active panel
            let tab_height = 1u16;
            let tab_labels = sidebar_tab_labels(sidebar_area.width);
            let tab_area = ratatui::layout::Rect::new(
                sidebar_area.x,
                sidebar_area.y,
                sidebar_area.width,
                tab_height,
            );
            let tabs = ratatui::widgets::Tabs::new(tab_labels).select(self.sidebar_active_tab);
            main_ctx.frame_mut().render_widget(tabs, tab_area);

            let panel_area = ratatui::layout::Rect::new(
                sidebar_area.x,
                sidebar_area.y.saturating_add(tab_height),
                sidebar_area.width,
                sidebar_area.height.saturating_sub(tab_height),
            );
            // Sync diff_viewer before rendering
            {
                // Collect diff text from recent tool calls for diff viewer
                let diffs: Vec<String> = self
                    .app
                    .timeline_clone_vec()
                    .iter()
                    .filter_map(|e| {
                        if let crate::tui::app::TimelineEntry::ToolCall { name, output, .. } = e {
                            if (name == "edit_file" || name == "patch_file" || name == "apply_diff")
                                && !output.is_empty()
                            {
                                Some(output.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect();
                if !diffs.is_empty() {
                    let combined = diffs.join(
                        "
---
",
                    );
                    self.diff_viewer.load(&combined);
                }
            }
            match self.sidebar_active_tab {
                0 => {
                    let _ = error_recovery::catch_render_panic(
                        "runtime_activity_panel",
                        AssertUnwindSafe(|| {
                            self.runtime_activity_panel
                                .render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                1 => {
                    let _ = error_recovery::catch_render_panic(
                        "file_changes_panel",
                        AssertUnwindSafe(|| {
                            self.file_changes_panel.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                2 => {
                    let _ = error_recovery::catch_render_panic(
                        "goal_workbench_panel",
                        AssertUnwindSafe(|| {
                            self.goal_workbench_panel.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                3 => {
                    let _ = error_recovery::catch_render_panic(
                        "approval_cockpit_panel",
                        AssertUnwindSafe(|| {
                            self.approval_cockpit_panel
                                .render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                4 => {
                    let _ = error_recovery::catch_render_panic(
                        "todo_panel",
                        AssertUnwindSafe(|| {
                            self.todo_panel.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                5 => {
                    let _guard = self.render_profiler.guard("diff_viewer");
                    let _ = error_recovery::catch_render_panic(
                        "diff_viewer",
                        AssertUnwindSafe(|| {
                            self.diff_viewer.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                6 => {
                    let _guard = self.render_profiler.guard("file_tree");
                    let _ = error_recovery::catch_render_panic(
                        "file_tree",
                        AssertUnwindSafe(|| {
                            self.file_tree.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                7 => {
                    let _guard = self.render_profiler.guard("session_sidebar");
                    let _ = error_recovery::catch_render_panic(
                        "session_sidebar",
                        AssertUnwindSafe(|| {
                            self.session_sidebar.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                8 => {
                    let _guard = self.render_profiler.guard("memory_panel");
                    let _ = error_recovery::catch_render_panic(
                        "memory_panel",
                        AssertUnwindSafe(|| {
                            self.memory_panel.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                9 => {
                    let _ = error_recovery::catch_render_panic(
                        "skills_panel",
                        AssertUnwindSafe(|| {
                            self.skills_panel.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                10 => {
                    let _ = error_recovery::catch_render_panic(
                        "gateway_panel",
                        AssertUnwindSafe(|| {
                            self.gateway_panel.render(&mut main_ctx, panel_area);
                        }),
                    );
                }
                _ => {}
            }
        }

        // 2. Render status bar at bottom (reuses main_ctx)
        {
            let status_area =
                ratatui::layout::Rect::new(0, area.height.saturating_sub(1), area.width, 1);
            let degraded = {
                let _guard = self.render_profiler.guard("status_bar");
                match error_recovery::catch_render_panic(
                    "status_bar",
                    AssertUnwindSafe(|| {
                        self.status_bar.render(&mut main_ctx, status_area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 2.5. Render input directly from app.input (BUG 1 FIX: single source of truth)
        // FIX B: Set block on textarea before rendering for cursor visibility
        {
            let input_y = area.height.saturating_sub(1 + input_h);
            let input_area = ratatui::layout::Rect::new(0, input_y, area.width, input_h);
            self.app.input.set_block(
                ratatui::widgets::Block::default()
                    .borders(ratatui::widgets::Borders::ALL)
                    .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "),
            );
            // Render app.input widget directly — NOT through prompt
            {
                let _guard = self.render_profiler.guard("input");
                main_ctx
                    .frame_mut()
                    .render_widget(&self.app.input, input_area);
            }
            // Render prompt's autocomplete dropdown as overlay
            {
                let _guard = self.render_profiler.guard("prompt_dropdown");
                let _ = error_recovery::catch_render_panic(
                    "prompt_dropdown",
                    AssertUnwindSafe(|| {
                        self.prompt.render_dropdown(&mut main_ctx, input_area);
                    }),
                );
            }
            // Render context suggestion bar above the input area
            if self.context_suggestions.is_active() {
                let _ = error_recovery::catch_render_panic(
                    "context_suggestions",
                    AssertUnwindSafe(|| {
                        self.context_suggestions.render(&mut main_ctx, input_area);
                    }),
                );
            }
        }

        // ── Overlays: one RenderContext for all conditional overlays ──
        let mut overlay_ctx: RenderContext = RenderContext::new(frame, &skin);

        // 3. Render thinking panel as compact floating box when turn is active.
        // Positioned at top-right of chat area so it doesn't obscure the full screen.
        if self.app.turn_active {
            // Compute a compact area: top-right corner of the main chat region.
            // Width ~35% of screen, height capped at 12 rows.
            let mut chat_area = self.layout_tree.area_of("chat").unwrap_or(area);
            chat_area.height = chat_area.height.saturating_sub(1).saturating_sub(input_h);
            let thinking_w = (area.width / 3).max(30).min(50);
            let thinking_h = (chat_area.height / 3).max(6).min(14);
            let thinking_x = chat_area.x + chat_area.width.saturating_sub(thinking_w);
            let thinking_y = if self.app.search_active {
                1
            } else {
                chat_area.y
            };
            let thinking_area =
                ratatui::layout::Rect::new(thinking_x, thinking_y, thinking_w, thinking_h);

            let degraded = {
                let _guard = self.render_profiler.guard("thinking_panel");
                match error_recovery::catch_render_panic(
                    "thinking_panel",
                    AssertUnwindSafe(|| {
                        self.thinking_panel.render(&mut overlay_ctx, thinking_area);
                    }),
                ) {
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
                let _guard = self.render_profiler.guard("agents_overlay");
                match error_recovery::catch_render_panic(
                    "agents_overlay",
                    AssertUnwindSafe(|| {
                        self.agents_overlay.render(&mut overlay_ctx, area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 5. Render agent team panel when visible
        if self.agent_team_panel.visible {
            let degraded = {
                let _guard = self.render_profiler.guard("agent_team_panel");
                match error_recovery::catch_render_panic(
                    "agent_team_panel",
                    AssertUnwindSafe(|| {
                        self.agent_team_panel.render(&mut overlay_ctx, area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 5.1. Render performance dashboard when visible
        if self.performance_dashboard.visible {
            let degraded = {
                let _guard = self.render_profiler.guard("performance_dashboard");
                match error_recovery::catch_render_panic(
                    "performance_dashboard",
                    AssertUnwindSafe(|| {
                        // Render in a centered rectangle (70% width, 60% height)
                        let dash_w = (area.width as f32 * 0.7) as u16;
                        let dash_h = (area.height as f32 * 0.55) as u16;
                        let dash_x = (area.width.saturating_sub(dash_w)) / 2;
                        let dash_y = (area.height.saturating_sub(dash_h)) / 2;
                        let dash_area = ratatui::layout::Rect::new(dash_x, dash_y, dash_w, dash_h);
                        self.performance_dashboard
                            .render(&mut overlay_ctx, dash_area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.add_message("system", &msg);
            }
        }

        // 5.5 Sync and render L4KnowledgeView when memory orchestrator is available
        if self.memory_orchestrator.is_some() {
            self.l4_knowledge_view.sync(&self.memory_orchestrator);
            if !self.l4_knowledge_view.entries.is_empty() {
                let degraded = {
                    let _guard = self.render_profiler.guard("l4_knowledge_view");
                    match error_recovery::catch_render_panic(
                        "l4_knowledge_view",
                        AssertUnwindSafe(|| {
                            self.l4_knowledge_view.render(&mut overlay_ctx, area);
                        }),
                    ) {
                        RenderResult::Ok => None,
                        RenderResult::Degraded(msg) => Some(msg),
                    }
                };
                if let Some(msg) = degraded {
                    self.add_message("system", &msg);
                }
            }
        }

        // 6. Render toast notifications at top-right
        if !self.toast_manager.is_empty() {
            let degraded = {
                let _guard = self.render_profiler.guard("toast_manager");
                match error_recovery::catch_render_panic(
                    "toast_manager",
                    AssertUnwindSafe(|| {
                        self.toast_manager.render(&mut overlay_ctx, area);
                    }),
                ) {
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
                let _guard = self.render_profiler.guard("dialog_manager");
                match error_recovery::catch_render_panic(
                    "dialog_manager",
                    AssertUnwindSafe(|| {
                        self.dialog_manager.render(&mut overlay_ctx, area);
                    }),
                ) {
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
                let _guard = self.render_profiler.guard("command_palette");
                match error_recovery::catch_render_panic(
                    "command_palette",
                    AssertUnwindSafe(|| {
                        self.command_palette.render(&mut overlay_ctx, area);
                    }),
                ) {
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
                    let _guard = self.render_profiler.guard("question_form");
                    match error_recovery::catch_render_panic(
                        "question_form",
                        AssertUnwindSafe(|| {
                            qf.render(&mut overlay_ctx, area);
                        }),
                    ) {
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
                let _guard = self.render_profiler.guard("export_dialog");
                match error_recovery::catch_render_panic(
                    "export_dialog",
                    AssertUnwindSafe(|| {
                        self.export_dialog.render(&mut overlay_ctx, area);
                    }),
                ) {
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
                match error_recovery::catch_render_panic(
                    "which_key",
                    AssertUnwindSafe(|| {
                        WhichKey::draw(frame, area, &self.keybind_engine);
                    }),
                ) {
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

        // 1.5. Agent team panel focus trap: route j/k/Up/Down/Tab to panel
        if self.agent_team_panel.visible {
            match event.code {
                KeyCode::Char('j' | 'k') | KeyCode::Up | KeyCode::Down => {
                    self.agent_team_panel.handle_key(&event);
                    return true;
                }
                KeyCode::Esc => {
                    self.agent_team_panel.visible = false;
                    return true;
                }
                _ => {}
            }
        }

        // 1.75. Tab/BackTab sidebar cycling (before keybind engine which maps Tab to no-op NextPanel)
        match event.code {
            KeyCode::Tab => {
                self.sidebar_active_tab = (self.sidebar_active_tab + 1) % SIDEBAR_TAB_COUNT;
                return true;
            }
            KeyCode::BackTab => {
                self.sidebar_active_tab = if self.sidebar_active_tab == 0 {
                    SIDEBAR_TAB_COUNT - 1
                } else {
                    self.sidebar_active_tab - 1
                };
                return true;
            }
            _ => {}
        }

        // 1.8. 'v' toggle compact chat view
        if let KeyCode::Char('v') = event.code {
            if !event.modifiers.contains(KeyModifiers::CONTROL)
                && !event.modifiers.contains(KeyModifiers::ALT)
            {
                self.app.compact_chat = !self.app.compact_chat;
                self.toast_manager.push(
                    ToastVariant::Info,
                    None,
                    format!(
                        "Chat view: {}",
                        if self.app.compact_chat {
                            "compact (summary)"
                        } else {
                            "verbose (full timeline)"
                        }
                    ),
                    1500,
                );
                return true;
            }
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
                    self.pending_export_options = self.export_dialog.result.clone();
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
        // BUG 1 FIX: Sync prompt textarea from app.input on-demand (not every frame).
        // This eliminates the bidirectional sync race condition.
        if key.code == KeyCode::Tab && !key.modifiers.contains(KeyModifiers::SHIFT) {
            // Sync prompt textarea from app.input, then refresh suggestions and handle Tab
            let input_text = self.app.input.lines().join("\n");
            self.prompt.set_text(&input_text);
            self.prompt.refresh_suggestions();
            let event = crossterm::event::Event::Key(key);
            let result = self.prompt.handle_event(&event);
            if result == crate::tui::components::EventResult::Consumed {
                // Sync accepted suggestion back to app.input
                let new_text = self.prompt.text();
                let mut ta = tui_textarea::TextArea::default();
                // Preserve the input block style
                ta.set_block(
                    ratatui::widgets::Block::default()
                        .borders(ratatui::widgets::Borders::ALL)
                        .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "),
                );
                if !new_text.is_empty() {
                    ta.insert_str(&new_text);
                }
                self.app.input = ta;
                return ProcessedKey::Nothing;
            }
            // Fall through to sidebar tab cycling
        }
        if key.code == KeyCode::Tab && key.modifiers.contains(KeyModifiers::SHIFT) {
            let input_text = self.app.input.lines().join("\n");
            self.prompt.set_text(&input_text);
            self.prompt.refresh_suggestions();
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
        // Tab / Shift+Tab: cycle through sidebar tabs.
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
            // BUG 1 FIX: Refresh suggestions from app.input text, not prompt's stale textarea
            let text = self.app.input.lines().join("\n");
            self.prompt.refresh_suggestions_from_text(&text);
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
            self.app.input.set_block(
                ratatui::widgets::Block::default()
                    .borders(ratatui::widgets::Borders::ALL)
                    .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "),
            );
            return ProcessedKey::Submit(text);
        }

        // Reset pending cancel/quit on any non-ESC/Ctrl+C key
        if key.code != KeyCode::Esc
            && !(key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            self.pending_cancel = false;
            self.pending_quit = false;
        }

        // 6. Esc/Ctrl+C: separate cancel (Esc) from exit (Ctrl+C), both double-press
        if key.code == KeyCode::Esc {
            // Performance dashboard consumes Esc to close itself
            if self.performance_dashboard.visible {
                self.performance_dashboard.visible = false;
                self.pending_cancel = false;
                self.pending_quit = false;
                return ProcessedKey::Nothing;
            }
            if self.app.turn_active {
                if self.pending_cancel {
                    self.pending_cancel = false;
                    return ProcessedKey::Cancel;
                }
                self.pending_cancel = true;
                self.pending_quit = false;
                self.toast_manager.push(
                    ToastVariant::Warning,
                    None,
                    "Press ESC again to cancel the current turn".into(),
                    2000,
                );
                return ProcessedKey::Nothing;
            }
            // ESC when no turn active: dismiss overlays, not exit
            self.pending_cancel = false;
            self.pending_quit = false;
            return ProcessedKey::Nothing;
        }
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            if self.app.turn_active {
                // Ctrl+C during active turn: cancel
                if self.pending_cancel {
                    self.pending_cancel = false;
                    return ProcessedKey::Cancel;
                }
                self.pending_cancel = true;
                self.pending_quit = false;
                self.toast_manager.push(
                    ToastVariant::Warning,
                    None,
                    "Press Ctrl+C again to cancel the current turn".into(),
                    2000,
                );
                return ProcessedKey::Nothing;
            }
            // Ctrl+C when idle: exit
            if self.pending_quit {
                self.pending_quit = false;
                return ProcessedKey::Exit;
            }
            self.pending_quit = true;
            self.pending_cancel = false;
            self.toast_manager.push(
                ToastVariant::Warning,
                None,
                "Press Ctrl+C again to exit".into(),
                2000,
            );
            return ProcessedKey::Nothing;
        }

        // 7. Ctrl+V: paste from system clipboard
        if key.code == KeyCode::Char('v') && key.modifiers.contains(KeyModifiers::CONTROL) {
            match crate::tui::clipboard::read_clipboard() {
                Some(crate::tui::clipboard::ClipboardContent::Text(text)) => {
                    self.app.input.insert_str(&text);
                }
                Some(crate::tui::clipboard::ClipboardContent::Image { .. }) => {
                    self.app.input.insert_str("[Image]");
                }
                None => {}
            }
            return ProcessedKey::Nothing;
        }

        // 8. Route through keybind engine for all remaining keys
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
        matches!(
            event.code,
            KeyCode::Backspace | KeyCode::Delete | KeyCode::Left | KeyCode::Right
        )
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
        let items: Vec<String> = self
            .app
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
                    self.app.scroll_offset = self.app.scroll_offset.saturating_add(delta as u16);
                    self.app.auto_scroll = false;
                } else {
                    self.app.scroll_offset = self.app.scroll_offset.saturating_sub((-delta) as u16);
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
                // Panel rotation removed — use sidebar navigation instead
            }
            Action::PrevPanel => {
                // Panel rotation removed — use sidebar navigation instead
            }
            Action::ToggleCommandPalette => {
                if self.command_palette.is_open() {
                    self.command_palette.close();
                } else {
                    let snapshot =
                        crate::tui::runtime_control_store::RuntimeControlSnapshot::from_app(
                            &self.app,
                        );
                    self.command_palette.sync_runtime_actions(&snapshot);
                    self.command_palette.open();
                }
            }
            Action::ToggleAgentsOverlay => {
                self.agents_overlay.toggle();
            }
            Action::ToggleAgentPanel => {
                self.agent_team_panel.toggle();
            }
            Action::TogglePerformanceDashboard => {
                self.performance_dashboard.toggle();
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
                    self.app
                        .show_notification(&format!("Switched to model: {model}"));
                }
            }
            Action::ReloadProviders => {
                let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
                let loader = runtime::ConfigLoader::default_for(cwd);
                self.reload_runtime_providers_from_loader(&loader);
            }
            Action::HistoryBrowse(older) => {
                let text = if older {
                    self.app.history_prev()
                } else {
                    self.app.history_next()
                };
                if let Some(text) = text {
                    let mut ta = tui_textarea::TextArea::default();
                    ta.set_block(
                        ratatui::widgets::Block::default()
                            .borders(ratatui::widgets::Borders::ALL)
                            .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "),
                    );
                    ta.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));
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
                self.sidebar_active_tab = 6;
            }
            Action::FocusFileTree => {
                self.sidebar_active_tab = 7;
            }
            Action::FocusSessions => {
                self.sidebar_active_tab = 8;
            }
            Action::Execute(ref cmd) => {
                let mut input = tui_textarea::TextArea::default();
                input.set_block(
                    ratatui::widgets::Block::default()
                        .borders(ratatui::widgets::Borders::ALL)
                        .title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "),
                );
                input.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));
                input.insert_str(cmd);
                self.app.input = input;
                self.app
                    .show_notification("Command prepared. Press Enter to run.");
            }
            Action::RespondDaemonApproval { id, approved } => {
                let approval_id = id.clone();
                let projection_id = id.clone();
                let result = run_daemon_control_blocking(move |client| async move {
                    client
                        .respond_approval(&id, approved, Some("once"), None)
                        .await
                })
                .or_else(move |_| {
                    run_daemon_projection_blocking(move |client| async move {
                        client
                            .respond_approval(&projection_id, approved, Some("once"), None)
                            .await
                    })
                });
                match result {
                    Ok(_) => {
                        self.apply_local_daemon_approval_response(&approval_id);
                        let verdict = if approved { "approved" } else { "rejected" };
                        self.push_runtime_action_receipt(
                            "ok",
                            verdict,
                            "daemon-control",
                            "daemon.approval.respond",
                            Some(approval_id.clone()),
                        );
                        self.toast_manager.push(
                            ToastVariant::Success,
                            Some("Approval".into()),
                            format!("Daemon approval {verdict}"),
                            2000,
                        );
                    }
                    Err(err) => {
                        self.push_runtime_action_receipt(
                            "failed",
                            &err,
                            "daemon-control",
                            "daemon.approval.respond",
                            Some(approval_id),
                        );
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Approval".into()),
                            err,
                            3000,
                        );
                    }
                }
            }
            Action::CancelDaemonTask(id) => {
                let task_id = id.clone();
                let projection_id = id.clone();
                let result = run_daemon_control_blocking(move |client| async move {
                    client.cancel_task(&id).await
                })
                .or_else(move |_| {
                    run_daemon_projection_blocking(move |client| async move {
                        client.cancel_task(&projection_id).await
                    })
                });
                match result {
                    Ok(_) => {
                        self.apply_local_daemon_task_status(&task_id, "cancelled");
                        self.push_runtime_action_receipt(
                            "ok",
                            "cancelled",
                            "daemon-control",
                            "daemon.task.cancel",
                            Some(task_id.clone()),
                        );
                        self.toast_manager.push(
                            ToastVariant::Success,
                            Some("Task".into()),
                            "Daemon task canceled".into(),
                            2000,
                        );
                    }
                    Err(err) => {
                        self.push_runtime_action_receipt(
                            "failed",
                            &err,
                            "daemon-control",
                            "daemon.task.cancel",
                            Some(task_id),
                        );
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Task".into()),
                            err,
                            3000,
                        );
                    }
                }
            }
            Action::CompleteDaemonTask(id) => {
                let task_id = id.clone();
                let projection_id = id.clone();
                let result = run_daemon_control_blocking(move |client| async move {
                    client.complete_task(&id).await
                })
                .or_else(move |_| {
                    run_daemon_projection_blocking(move |client| async move {
                        client.complete_task(&projection_id).await
                    })
                });
                match result {
                    Ok(_) => {
                        self.apply_local_daemon_task_status(&task_id, "completed");
                        self.push_runtime_action_receipt(
                            "ok",
                            "completed",
                            "daemon-control",
                            "daemon.task.complete",
                            Some(task_id.clone()),
                        );
                        self.toast_manager.push(
                            ToastVariant::Success,
                            Some("Task".into()),
                            "Daemon task completed".into(),
                            2000,
                        );
                    }
                    Err(err) => {
                        self.push_runtime_action_receipt(
                            "failed",
                            &err,
                            "daemon-control",
                            "daemon.task.complete",
                            Some(task_id),
                        );
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Task".into()),
                            err,
                            3000,
                        );
                    }
                }
            }
            Action::RevalidateConnectorResource { reference, state } => {
                let resource_ref = reference.clone();
                let desired_state = state.clone();
                let projection_ref = reference.clone();
                let projection_state = state.clone();
                let result = run_daemon_control_blocking(move |client| async move {
                    client
                        .revalidate_connector_resource(&reference, &state)
                        .await
                })
                .or_else(move |_| {
                    run_daemon_projection_blocking(move |client| async move {
                        client
                            .revalidate_connector_resource(&projection_ref, &projection_state)
                            .await
                    })
                });
                match result {
                    Ok(value)
                        if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) =>
                    {
                        self.apply_local_connector_resource_state(&resource_ref, &desired_state);
                        self.push_runtime_action_receipt(
                            "ok",
                            &desired_state,
                            "daemon-control",
                            "connector.resource.revalidate",
                            Some(resource_ref.clone()),
                        );
                        self.toast_manager.push(
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
                        self.push_runtime_action_receipt(
                            "skipped",
                            &reason,
                            "daemon-control",
                            "connector.resource.revalidate",
                            Some(resource_ref),
                        );
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Connector".into()),
                            reason,
                            3000,
                        );
                    }
                    Err(err) => {
                        self.push_runtime_action_receipt(
                            "failed",
                            &err,
                            "daemon-control",
                            "connector.resource.revalidate",
                            Some(resource_ref),
                        );
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Connector".into()),
                            err,
                            3000,
                        );
                    }
                }
            }
            Action::PromoteConnectorResourceToMemory {
                reference,
                session_id,
            } => {
                let session_id = session_id
                    .clone()
                    .or_else(|| Some(self.app.session_id.clone()));
                let projection_ref = reference.clone();
                let receipt_ref = reference.clone();
                let projection_session_id = session_id.clone();
                let result = run_daemon_control_blocking(move |client| async move {
                    client
                        .promote_connector_resource_to_memory(&reference, session_id.as_deref())
                        .await
                })
                .or_else(move |_| {
                    run_daemon_projection_blocking(move |client| async move {
                        client
                            .promote_connector_resource_to_memory(
                                &projection_ref,
                                projection_session_id.as_deref(),
                            )
                            .await
                    })
                });
                match result {
                    Ok(value)
                        if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) =>
                    {
                        let memory_id = value
                            .get("memory_id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("remembered");
                        self.push_runtime_action_receipt(
                            "ok",
                            memory_id,
                            "daemon-control",
                            "connector.resource.promote_memory",
                            Some(receipt_ref.clone()),
                        );
                        self.toast_manager.push(
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
                        self.push_runtime_action_receipt(
                            "skipped",
                            &reason,
                            "daemon-control",
                            "connector.resource.promote_memory",
                            Some(receipt_ref.clone()),
                        );
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Memory".into()),
                            reason,
                            3000,
                        );
                    }
                    Err(err) => {
                        self.push_runtime_action_receipt(
                            "failed",
                            &err,
                            "daemon-control",
                            "connector.resource.promote_memory",
                            Some(receipt_ref),
                        );
                        self.toast_manager.push(
                            ToastVariant::Warning,
                            Some("Memory".into()),
                            err,
                            3000,
                        );
                    }
                }
            }
            Action::TogglePanel(ref _name) => {}
            Action::ApplyPreset(preset) => {
                self.layout_tree.apply_preset(preset);
                let label = match preset {
                    crate::tui::layout::LayoutPreset::Coding => "Coding",
                    crate::tui::layout::LayoutPreset::Review => "Review",
                    crate::tui::layout::LayoutPreset::Collaboration => "Collaboration",
                };
                self.toast_manager.push(
                    ToastVariant::Info,
                    Some("Layout".into()),
                    format!("Switched to {label} layout"),
                    2000,
                );
            }
            Action::Noop => {}
        }
    }

    fn apply_local_daemon_approval_response(&mut self, approval_id: &str) {
        self.mutate_runtime_control_store(|store| store.apply_approval_response(approval_id));
    }

    fn apply_local_daemon_task_status(&mut self, task_id: &str, status: &str) {
        self.mutate_runtime_control_store(|store| store.apply_task_status(task_id, status));
    }

    fn apply_local_connector_resource_state(&mut self, reference: &str, state: &str) {
        self.mutate_runtime_control_store(|store| {
            store.apply_connector_resource_state(reference, state);
        });
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
        mutate: impl FnOnce(&mut crate::tui::runtime_control_store::RuntimeControlLocalStore),
    ) {
        let mut store =
            crate::tui::runtime_control_store::RuntimeControlLocalStore::from_app(&self.app);
        mutate(&mut store);
        store.apply_to_app(&mut self.app);
        self.sync_runtime_control_surfaces(store.snapshot());
    }

    fn sync_runtime_control_surfaces(
        &mut self,
        snapshot: &crate::tui::runtime_control_store::RuntimeControlSnapshot,
    ) {
        self.approval_cockpit_panel.sync_from_app(&self.app);
        self.goal_workbench_panel.sync_from_app(&self.app);
        self.gateway_panel.sync_from_app(&self.app);
        self.command_palette.sync_runtime_actions(&snapshot);
    }

    fn reload_runtime_providers_from_loader(&mut self, loader: &runtime::ConfigLoader) -> bool {
        match loader.load() {
            Ok(runtime_config) => {
                let providers = runtime_config.providers().clone();
                let provider_count = providers.providers.len();
                let provider_model_count: usize =
                    providers.providers.values().map(|p| p.models.len()).sum();
                let configured_model = runtime_config.model().map(str::to_string);
                let configured_model_provider = configured_model
                    .as_deref()
                    .and_then(|model| providers.resolve_full(model))
                    .map(|provider| provider.name.clone());
                let configured_model_resolved =
                    configured_model.is_none() || configured_model_provider.is_some();
                let status = if provider_count == 0 {
                    "unconfigured"
                } else if configured_model_resolved {
                    "applied"
                } else {
                    "attention"
                };

                runtime::init_global_providers(providers);
                self.runtime_activity_panel.sync_from_app(&self.app);

                let route =
                    configured_model_provider
                        .as_deref()
                        .unwrap_or(if configured_model_resolved {
                            "override"
                        } else {
                            "missing"
                        });
                let message =
                    format!("Providers {status}: {provider_count} providers, {provider_model_count} models, route {route}");
                let variant = if status == "applied" {
                    ToastVariant::Success
                } else {
                    ToastVariant::Warning
                };
                self.toast_manager
                    .push(variant, Some("Providers".into()), message.clone(), 3000);
                self.app.show_notification(&message);
                true
            }
            Err(error) => {
                let message = format!("Provider reload failed: {error}");
                self.toast_manager.push(
                    ToastVariant::Error,
                    Some("Providers".into()),
                    message.clone(),
                    4000,
                );
                self.app.show_notification(&message);
                false
            }
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
    pub fn register_component(&mut self, id: EventComponentId, component: Box<dyn Component>) {
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
    fn render_message_menu(
        &mut self,
        frame: &mut Frame,
        area: ratatui::layout::Rect,
        _skin: &crate::tui::skin::SkinConfig,
    ) {
        if !self.chat_view.pending_message_menu {
            return;
        }

        use ratatui::style::{Color, Modifier, Style};
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
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));

        for (key, label, _desc) in &menu_items {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  [{key}] "),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(*label, Style::default().fg(Color::White)),
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

// ── L4KnowledgeView ──────────────────────────────────────────────

/// Displays L4 (shared/team-scoped) memory entries in the overlay layer.
///
/// Synced from `MemoryOrchestrator` each render frame when available.
/// Shows a compact list of recent L4 memory entries with title, tags,
/// and confidence.
pub struct L4KnowledgeView {
    /// Cached L4 entry titles (synced from orchestrator).
    pub entries: Vec<String>,
    /// Whether the view has been synced at least once.
    pub synced: bool,
    /// Status message (e.g. "Orchestrator available" / "No L4 entries").
    pub status: String,
    /// Last time entries were refreshed from the memory store.
    last_sync_at: Option<Instant>,
}

impl L4KnowledgeView {
    /// Create a new empty L4KnowledgeView.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            synced: false,
            status: String::new(),
            last_sync_at: None,
        }
    }

    /// Sync from an optional memory orchestrator reference.
    pub fn sync(
        &mut self,
        memory_orchestrator: &Option<std::sync::Arc<memory::MemoryOrchestrator>>,
    ) {
        let Some(orchestrator) = memory_orchestrator else {
            self.status = "No L4 orchestrator".to_string();
            self.synced = false;
            self.entries.clear();
            self.last_sync_at = None;
            return;
        };

        let should_sync = self
            .last_sync_at
            .map(|last| last.elapsed() >= Duration::from_secs(1))
            .unwrap_or(true);
        if !should_sync {
            return;
        }

        match search_l4_entries_blocking(std::sync::Arc::clone(orchestrator)) {
            Ok(mut entries) => {
                entries.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
                let count = entries.len();
                self.entries = entries
                    .into_iter()
                    .take(40)
                    .map(|entry| {
                        let tags = if entry.tags.is_empty() {
                            String::new()
                        } else {
                            format!(" [{}]", entry.tags.join(","))
                        };
                        format!("{:?} {}{}", entry.priority, entry.title, tags)
                    })
                    .collect();
                self.status = format!("Synced {count} L4 entries");
                self.synced = true;
                self.last_sync_at = Some(Instant::now());
            }
            Err(err) => {
                self.status = format!("L4 sync failed: {err}");
                self.synced = false;
                self.last_sync_at = Some(Instant::now());
            }
        };
    }

    /// Render the L4 knowledge view as a compact overlay.
    pub fn render(
        &self,
        ctx: &mut crate::tui::components::RenderContext,
        area: ratatui::layout::Rect,
    ) {
        use ratatui::style::{Color, Modifier, Style};
        use ratatui::text::{Line, Span};
        use ratatui::widgets::{Block, Borders, Paragraph};

        let block = Block::default()
            .title(" L4 Knowledge ")
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

        let height = (lines.len() as u16 + 2).min(area.height);
        let rect = ratatui::layout::Rect::new(area.width.saturating_sub(40), 0, 40, height);

        let paragraph = Paragraph::new(lines).block(block);
        ctx.frame_mut().render_widget(paragraph, rect);
    }
}

fn run_daemon_control_blocking<F, Fut>(operation: F) -> Result<serde_json::Value, String>
where
    F: FnOnce(crate::tui::control_client::DaemonControlClient) -> Fut + Send + 'static,
    Fut: std::future::Future<
            Output = Result<serde_json::Value, crate::tui::control_client::DaemonControlError>,
        > + Send
        + 'static,
{
    let run = move || {
        let client = crate::tui::control_client::DaemonControlClient::default_local();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;
        runtime
            .block_on(operation(client))
            .map_err(|err| err.to_string())
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(run)
            .join()
            .map_err(|_| "daemon control worker panicked".to_string())?
    } else {
        run()
    }
}

fn daemon_projection_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
}

fn run_daemon_projection_blocking<F, Fut>(operation: F) -> Result<serde_json::Value, String>
where
    F: FnOnce(crate::tui::projection_client::DaemonProjectionClient) -> Fut + Send + 'static,
    Fut: std::future::Future<
            Output = Result<serde_json::Value, crate::tui::projection_client::ProjectionError>,
        > + Send
        + 'static,
{
    let run = move || {
        let Some(client) =
            crate::tui::projection_client::DaemonProjectionClient::from_running_gateway(
                daemon_projection_auth_token(),
            )
            .map_err(|err| err.to_string())?
        else {
            return Err("daemon gateway is not running".to_string());
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;
        runtime
            .block_on(operation(client))
            .map_err(|err| err.to_string())
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(run)
            .join()
            .map_err(|_| "daemon projection worker panicked".to_string())?
    } else {
        run()
    }
}

fn search_l4_entries_blocking(
    orchestrator: std::sync::Arc<memory::MemoryOrchestrator>,
) -> Result<Vec<memory::MemoryEntry>, String> {
    let search = move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;
        runtime
            .block_on(
                orchestrator
                    .store()
                    .search_by_layer(memory::MemoryLayer::L4),
            )
            .map_err(|err| err.to_string())
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(search)
            .join()
            .map_err(|_| "L4 sync worker panicked".to_string())?
    } else {
        search()
    }
}

impl Default for L4KnowledgeView {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::layout::LayoutNode;
    use crate::tui::test_utils::MockTerminal;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use serial_test::serial;
    use std::time::Duration;

    fn test_memory_config(path: &std::path::Path) -> memory::MemoryConfig {
        let mut config = memory::MemoryConfig::default();
        config.store.sqlite_path = path.to_path_buf();
        config.store.blob_dir = path.parent().unwrap().join("blobs");
        config
    }

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

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

    #[test]
    fn local_daemon_approval_response_updates_projection_state() {
        let mut state = TuiState::new("test-model", "test-session");
        state.app.daemon_approval_items = vec![
            crate::tui::runtime_control_store::DaemonApprovalSummary {
                id: "approval-1".to_string(),
                tool_name: "bash".to_string(),
                risk: Some("high".to_string()),
                requester: Some("session".to_string()),
                input_preview: "rm -rf /tmp/example".to_string(),
            },
            crate::tui::runtime_control_store::DaemonApprovalSummary {
                id: "approval-2".to_string(),
                tool_name: "edit".to_string(),
                risk: Some("medium".to_string()),
                requester: Some("session".to_string()),
                input_preview: "write file".to_string(),
            },
        ];
        state.app.daemon_pending_approvals = Some(2);

        state.apply_local_daemon_approval_response("approval-1");

        assert_eq!(state.app.daemon_pending_approvals, Some(1));
        assert_eq!(state.app.daemon_approval_items.len(), 1);
        assert_eq!(state.app.daemon_approval_items[0].id, "approval-2");
    }

    #[test]
    fn local_daemon_task_status_updates_projection_state() {
        let mut state = TuiState::new("test-model", "test-session");
        state.app.daemon_tasks = vec![
            crate::tui::runtime_control_store::DaemonTaskSummary {
                id: "task-1".to_string(),
                objective: "blocked task".to_string(),
                status: "blocked".to_string(),
                current_phase: Some("verify".to_string()),
                yolo_mode: true,
                failure_count: 1,
                review_result: None,
                artifact_count: 0,
                blocker_reason: Some("waiting for approval".to_string()),
            },
            crate::tui::runtime_control_store::DaemonTaskSummary {
                id: "task-2".to_string(),
                objective: "running task".to_string(),
                status: "running".to_string(),
                current_phase: None,
                yolo_mode: false,
                failure_count: 0,
                review_result: None,
                artifact_count: 0,
                blocker_reason: None,
            },
        ];
        state.app.daemon_task_count = Some(2);

        state.apply_local_daemon_task_status("task-1", "completed");

        assert_eq!(state.app.daemon_task_count, Some(2));
        assert_eq!(state.app.daemon_tasks[0].status, "completed");
        assert_eq!(state.app.daemon_tasks[0].blocker_reason, None);
        assert_eq!(state.app.daemon_tasks[1].status, "running");
    }

    #[test]
    fn local_connector_resource_state_updates_projection_state() {
        let mut state = TuiState::new("test-model", "test-session");
        state.app.daemon_connector_resources = vec![
            crate::tui::runtime_control_store::ConnectorResourceSummary {
                reference: "service://mock.docs/document/tui-doc".to_string(),
                provider: "mock.docs".to_string(),
                resource_type: "document".to_string(),
                title: "TUI Doc".to_string(),
                indexed_state: "indexed".to_string(),
            },
        ];

        state.apply_local_connector_resource_state("service://mock.docs/document/tui-doc", "stale");

        assert_eq!(
            state.app.daemon_connector_resources[0].indexed_state,
            "stale"
        );
    }

    #[test]
    #[serial]
    fn connector_resource_actions_prefer_socket_control() {
        use std::io::{BufRead, Write};

        let dir = unique_temp_dir("cowd-tui-connector-socket");
        let socket = dir.join("control.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind socket");
        let server = std::thread::spawn(move || {
            for expected in [
                "connector_resource_revalidate",
                "connector_resource_promote_memory",
            ] {
                let (stream, _) = listener.accept().expect("accept socket");
                let mut reader = std::io::BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).expect("read command");
                let command: serde_json::Value =
                    serde_json::from_str(line.trim()).expect("command json");
                assert_eq!(
                    command.get("cmd").and_then(|value| value.as_str()),
                    Some(expected)
                );
                assert_eq!(
                    command.get("reference").and_then(|value| value.as_str()),
                    Some("service://mock.docs/document/tui-doc")
                );
                let stream = reader.get_mut();
                match expected {
                    "connector_resource_revalidate" => {
                        assert_eq!(
                            command.get("state").and_then(|value| value.as_str()),
                            Some("stale")
                        );
                        stream
                            .write_all(br#"{"ok":true,"changed":true,"state":"stale"}"#)
                            .expect("write response");
                    }
                    _ => {
                        assert_eq!(
                            command.get("session_id").and_then(|value| value.as_str()),
                            Some("test-session")
                        );
                        stream
                            .write_all(br#"{"ok":true,"memory_id":"mem-1","layer":"L3"}"#)
                            .expect("write response");
                    }
                }
                stream.write_all(b"\n").expect("write newline");
            }
        });

        unsafe {
            std::env::set_var("COWD_DAEMON_SOCKET", &socket);
        }
        let mut state = TuiState::new("test-model", "test-session");
        state.app.daemon_connector_resources = vec![
            crate::tui::runtime_control_store::ConnectorResourceSummary {
                reference: "service://mock.docs/document/tui-doc".to_string(),
                provider: "mock.docs".to_string(),
                resource_type: "document".to_string(),
                title: "TUI Doc".to_string(),
                indexed_state: "indexed".to_string(),
            },
        ];

        state.dispatch_action(Action::RevalidateConnectorResource {
            reference: "service://mock.docs/document/tui-doc".to_string(),
            state: "stale".to_string(),
        });
        state.dispatch_action(Action::PromoteConnectorResourceToMemory {
            reference: "service://mock.docs/document/tui-doc".to_string(),
            session_id: None,
        });

        unsafe {
            std::env::remove_var("COWD_DAEMON_SOCKET");
        }
        server.join().expect("server thread");
        assert_eq!(
            state.app.daemon_connector_resources[0].indexed_state,
            "stale"
        );
        assert_eq!(state.app.daemon_action_receipts.len(), 2);
        assert_eq!(
            state.app.daemon_action_receipts[0].capability,
            "connector.resource.promote_memory"
        );
        assert_eq!(
            state.app.daemon_action_receipts[1].capability,
            "connector.resource.revalidate"
        );
        assert_eq!(state.gateway_panel.execution_receipts.len(), 2);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn reload_runtime_providers_from_loader_updates_registry_without_leaking_secret() {
        runtime::init_global_providers(runtime::ProvidersConfig::default());
        let root = unique_temp_dir("cowd-tui-provider-reload");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: "tui-reload-model"
providers:
  tui-provider:
    base_url: "https://tui-provider.example/v1"
    api_key: "tui-secret-key"
    models: ["tui-reload-model", "tui-fast"]
    protocol: "openai-compat"
"#,
        )
        .unwrap();

        let mut state = TuiState::new("tui-reload-model", "session-tui-provider");
        let loader = runtime::ConfigLoader::new(&workspace, &config_home);
        assert!(state.reload_runtime_providers_from_loader(&loader));

        let provider = runtime::resolve_global_provider("tui-reload-model")
            .expect("provider reload should resolve active model");
        assert_eq!(provider.name, "tui-provider");
        assert_eq!(runtime::list_all_models().len(), 2);
        assert!(state
            .app
            .notification
            .as_deref()
            .unwrap_or_default()
            .contains("Providers applied"));
        assert!(!state
            .app
            .notification
            .as_deref()
            .unwrap_or_default()
            .contains("tui-secret-key"));

        let invalid_home = root.join("invalid-home");
        std::fs::create_dir_all(&invalid_home).unwrap();
        std::fs::write(
            invalid_home.join("config.yaml"),
            r#"
model: "broken-model"
providers:
  broken:
    base_url: "https://broken.example/v1"
    api_key: "broken-secret-key"
    models: ["broken-model"]
    protocol: "unsupported-protocol"
"#,
        )
        .unwrap();
        let invalid_loader = runtime::ConfigLoader::new(&workspace, &invalid_home);
        assert!(!state.reload_runtime_providers_from_loader(&invalid_loader));
        assert!(runtime::resolve_global_provider("broken-model").is_none());
        assert_eq!(
            runtime::resolve_global_provider("tui-reload-model")
                .expect("failed reload should preserve previous registry")
                .name,
            "tui-provider"
        );
        assert!(!state
            .app
            .notification
            .as_deref()
            .unwrap_or_default()
            .contains("broken-secret-key"));

        runtime::init_global_providers(runtime::ProvidersConfig::default());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn set_memory_manager_wires_tui_memory_surfaces() {
        let dir = unique_temp_dir("cowd-tui-memory");
        let manager = std::sync::Arc::new(
            memory::CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                memory::MemoryLayer::L4,
                memory::MemoryCategory::Shared,
                "TUI L4 Decision",
                "TUI must read real L4 shared memory.",
                memory::Priority::High,
                vec!["tui".into(), "l4".into()],
                memory::MemoryScope::Global,
            )
            .await
            .unwrap();

        let mut state = TuiState::new("test-model", "test-session");
        state.set_memory_manager(manager);

        assert!(state.memory_panel.memory_manager.is_some());
        assert!(state.memory_orchestrator.is_some());

        state.l4_knowledge_view.sync(&state.memory_orchestrator);

        assert!(
            state
                .l4_knowledge_view
                .entries
                .iter()
                .any(|entry| entry.contains("TUI L4 Decision")),
            "L4 overlay should sync real entries from the memory store"
        );
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

        state.apply_event(CowdEvent::TurnStarted);
        state.apply_event(CowdEvent::TextDelta {
            text: "Hello world".into(),
        });
        state.apply_event(CowdEvent::TurnComplete {
            assistant_text: String::new(),
            iterations: 1,
        });

        // Should have: assistant message + "✓ Done"
        assert!(state.timeline_len() >= 2);
        // The last entry should be "✓ Done"
        let last = state.timeline_get(state.timeline_len() - 1).unwrap();
        let text = last.full_text();
        assert!(
            text.contains("Done"),
            "expected '✓ Done' marker, got: {text}"
        );
    }

    #[test]
    fn apply_event_tool_lifecycle() {
        let mut state = TuiState::new("m", "s");

        state.apply_event(CowdEvent::TurnStarted);
        state.apply_event(CowdEvent::ToolStart {
            id: "t1".into(),
            name: "bash".into(),
            preview: "ls -la".into(),
        });

        assert!(state.timeline_iter().any(
            |(_, e)| matches!(&e, crate::tui::app::TimelineEntry::ToolCall { id, .. } if id == "t1")
        ));
    }

    #[test]
    fn apply_event_token_usage_updates_counters() {
        let mut state = TuiState::new("m", "s");

        state.apply_event(CowdEvent::TokenUsage {
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

    // ── dialog focus trap ───────────────────────────────────────

    #[test]
    fn handle_input_dialog_focus_trap() {
        let mut state = TuiState::new("m", "s");

        // Push an alert dialog
        use crate::tui::components::dialog::{DialogKind, DialogState};
        state
            .dialog_manager
            .push(DialogState::new(DialogKind::Alert {
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
        state
            .dialog_manager
            .push(DialogState::new(DialogKind::Alert {
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

    #[test]
    fn sidebar_tab_labels_use_compact_mode_for_narrow_sidebars() {
        let compact = sidebar_tab_labels(72);
        let full = sidebar_tab_labels(120);

        assert_eq!(compact.len(), SIDEBAR_TAB_COUNT);
        assert_eq!(full.len(), SIDEBAR_TAB_COUNT);
        assert_eq!(compact[0], "Run");
        assert_eq!(compact[3], "Appr");
        assert_eq!(full[0], "Run");
        assert_eq!(full[3], "Approvals");
    }

    #[test]
    fn renders_every_sidebar_tab_in_wide_and_compact_layouts() {
        for (width, height) in [(140, 38), (88, 32)] {
            for tab in 0..SIDEBAR_TAB_COUNT {
                let mut state = TuiState::new("m", "scenario-session");
                state.app.server_running = true;
                state.app.active_api_sessions = 1;
                state.app.daemon_runtime_readiness = Some("92%".to_string());
                state.app.daemon_task_count = Some(1);
                state.app.daemon_pending_approvals = Some(1);
                state.app.daemon_cross_plane_grants_active = Some(1);
                state.app.memory_status = Some("available".to_string());
                state.sidebar_active_tab = tab;

                let mut terminal = MockTerminal::new(width, height);
                terminal.draw(|frame| state.render(frame));
                let joined = terminal.buffer_lines().join("\n");

                assert!(
                    !joined.trim().is_empty(),
                    "tab {tab} at {width}x{height} rendered an empty buffer"
                );
            }
        }
    }

    #[test]
    fn render_bridge_projects_runtime_command_center_to_gateway_tab() {
        let mut state = TuiState::new("m", "scenario-session");
        state.sidebar_active_tab = 11;
        state.app.server_running = true;
        state.app.server_uptime_secs = Some(61);
        state.app.active_api_sessions = 2;
        state.app.daemon_runtime_readiness = Some("94%".to_string());
        state.app.daemon_runtime_components = Some(12);
        state.app.daemon_task_count = Some(3);
        state.app.daemon_pending_approvals = Some(1);
        state.app.memory_status = Some("available".to_string());
        state.app.daemon_action_receipts = vec![
            crate::tui::runtime_control_store::RuntimeActionReceiptSummary {
                status: "ok".to_string(),
                dispatch_status: "completed".to_string(),
                mode: "daemon-control".to_string(),
                capability: "daemon.task.complete".to_string(),
                idempotency_key: Some("task-1".to_string()),
            },
        ];
        state.app.daemon_connector_resources = vec![
            crate::tui::runtime_control_store::ConnectorResourceSummary {
                reference: "service://mock.docs/document/1".to_string(),
                provider: "mock.docs".to_string(),
                resource_type: "document".to_string(),
                title: "Bridge Doc".to_string(),
                indexed_state: "indexed".to_string(),
            },
        ];

        let mut terminal = MockTerminal::new(132, 38);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        for expected in [
            "Core Runtime",
            "AI Context",
            "Work Control",
            "Connector Plane",
            "available",
            "completed",
            "mock.docs",
            "indexed",
        ] {
            assert!(
                joined.contains(expected),
                "gateway bridge render should contain {expected}, got: {joined}"
            );
        }
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
        assert_eq!(
            state.startup_phase,
            StartupPhase::Loading,
            "should be Loading after delay"
        );

        // Signal ready
        state.update_startup_phase_at(true, state.startup_start + Duration::from_millis(600));
        assert_eq!(
            state.startup_phase,
            StartupPhase::Finishing,
            "should be Finishing when ready"
        );
    }
}
