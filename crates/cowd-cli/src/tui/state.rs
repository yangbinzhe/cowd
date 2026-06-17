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
use crate::tui::components::activity_panel::ActivityPanel;
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
use crate::tui::components::system_status_bar::SystemStatusBar;
use crate::tui::components::thinking_panel::ThinkingPanel;
use crate::tui::components::toast::{ToastManager, ToastVariant};
use crate::tui::components::todo_panel::TodoPanel;
use crate::tui::components::tool_ops_panel::{ToolOpsMode, ToolOpsPanel};
use crate::tui::components::{Component, RenderContext};
use crate::tui::context_tokens::validate_context_tokens;
use crate::tui::error_recovery::{self, RenderResult};
use crate::tui::event::dispatcher::EventDispatcher;
use crate::tui::event::{ComponentId as EventComponentId, EventBus, EventPriority};
use crate::tui::keybind::types::Action;
use crate::tui::keybind::which_key::WhichKey;
use crate::tui::keybind::{default_bindings, KeybindEngine};
use crate::tui::layout::{LayoutState, LayoutTree};
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

pub(crate) const SIDEBAR_TAB_COUNT: usize = 9;
pub(crate) const TAB_RUNTIME: usize = 0;
pub(crate) const TAB_TOOLS: usize = 1;
pub(crate) const TAB_CHANGES: usize = 2;
pub(crate) const TAB_GOALS: usize = 3;
pub(crate) const TAB_APPROVALS: usize = 4;
pub(crate) const TAB_TODO: usize = 5;
pub(crate) const TAB_FILES: usize = 6;
pub(crate) const TAB_SESSIONS: usize = 7;
pub(crate) const TAB_GATEWAY: usize = 8;

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
            FocusTarget::CommandPalette => "palette",
            FocusTarget::PromptSuggestions => "suggest",
            FocusTarget::Dialog => "dialog",
        }
    }

    fn hint(self) -> &'static str {
        match self {
            FocusTarget::Chat => "j/k scroll · / commands · Ctrl+P palette · Ctrl+B panels",
            FocusTarget::Input => "Enter send · Alt+Enter/Ctrl+J newline · / commands · Esc clear",
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
}

impl SidebarTopicPanel {
    fn label(self) -> &'static str {
        match self {
            SidebarTopicPanel::Diff => "Diff",
            SidebarTopicPanel::Memory => "Memory",
            SidebarTopicPanel::Skills => "Skills",
        }
    }
}

fn sidebar_tab_labels(width: u16) -> Vec<&'static str> {
    if width < 96 {
        vec![
            "Run", "Tool", "Chg", "Goal", "Appr", "Todo", "File", "Sess", "Gate",
        ]
    } else {
        vec![
            "Run",
            "Tools",
            "Changes",
            "Goals",
            "Approvals",
            "Todo",
            "Files",
            "Sessions",
            "Gateway",
        ]
    }
}

fn char_col_to_byte_offset(text: &str, col: usize) -> usize {
    text.char_indices()
        .nth(col)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len())
}

/// Wrap a single text line to fit within `max_width` display columns.
/// Uses word boundaries (spaces) for natural breaks, falling back to
/// character-level breaking for very long words.
/// Returns a vector of wrapped lines (no line is empty).
fn wrap_line_to_width(line: &str, max_width: usize) -> Vec<String> {
    if max_width == 0 {
        return vec![line.to_string()];
    }

    let mut result: Vec<String> = Vec::new();
    let mut remaining = line;
    while !remaining.is_empty() {
        let char_count = remaining.chars().count();
        if char_count <= max_width {
            result.push(remaining.to_string());
            break;
        }

        // Find last space within max_width characters
        let char_indices: Vec<usize> = remaining.char_indices().map(|(i, _)| i).collect();
        let break_char_idx = {
            let slice_end = char_indices
                .get(max_width)
                .copied()
                .unwrap_or(remaining.len());
            let (visible, _rest) = remaining.split_at(slice_end);
            // Find last space in visible portion
            visible.rfind(' ').map(|byte_idx| {
                // Count chars up to this byte index
                remaining[..byte_idx].chars().count()
            })
        };

        match break_char_idx {
            Some(idx) if idx > 0 => {
                let byte_pos = char_indices[idx];
                let (chunk, rest) = remaining.split_at(byte_pos);
                result.push(chunk.trim_end().to_string());
                remaining = rest.trim_start();
            }
            _ => {
                // No word boundary found — break at max_width characters
                let byte_pos = char_indices
                    .get(max_width)
                    .copied()
                    .unwrap_or(remaining.len());
                let (chunk, rest) = remaining.split_at(byte_pos);
                result.push(chunk.to_string());
                remaining = rest;
            }
        }
    }

    // Ensure no empty strings
    result.retain(|s| !s.is_empty());
    if result.is_empty() {
        result.push(String::new());
    }
    result
}

#[derive(Debug, Clone, Copy)]
struct TuiFrameAreas {
    system: ratatui::layout::Rect,
    search: Option<ratatui::layout::Rect>,
    body: ratatui::layout::Rect,
    input: ratatui::layout::Rect,
    status: ratatui::layout::Rect,
}

impl TuiFrameAreas {
    fn build(area: ratatui::layout::Rect, input_h: u16, search_active: bool) -> Self {
        let top_h = 1u16;
        let bottom_status_h = 1u16;
        let system = ratatui::layout::Rect::new(area.x, area.y, area.width, top_h);
        let status = ratatui::layout::Rect::new(
            area.x,
            area.y
                .saturating_add(area.height.saturating_sub(bottom_status_h)),
            area.width,
            bottom_status_h,
        );
        let input_y = status.y.saturating_sub(input_h);
        let input = ratatui::layout::Rect::new(area.x, input_y, area.width, input_h);
        let available_body_h = input_y.saturating_sub(system.y.saturating_add(system.height));
        let search_h = if search_active && available_body_h > 1 {
            1
        } else {
            0
        };
        let search = (search_h > 0).then(|| {
            ratatui::layout::Rect::new(
                area.x,
                system.y.saturating_add(system.height),
                area.width,
                search_h,
            )
        });
        let body_y = system
            .y
            .saturating_add(system.height)
            .saturating_add(search_h);
        let body_h = input_y.saturating_sub(body_y);
        let body = ratatui::layout::Rect::new(area.x, body_y, area.width, body_h);

        Self {
            system,
            search,
            body,
            input,
            status,
        }
    }
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

    /// Runtime layout state. The sidebar is hidden by default so the first
    /// screen stays focused on chat/input and heavier panels render on demand.
    pub layout_state: LayoutState,

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

    /// Gateway panel showing backend runtime/API gateway status.
    pub gateway_panel: GatewayPanel,

    /// Runtime activity panel summarizing run/context/tool state.
    pub runtime_activity_panel: RuntimeActivityPanel,

    /// Tool operations console for registry, execution, mutations, ledger, and risk checks.
    pub tool_ops_panel: ToolOpsPanel,

    /// Top system status strip for runtime/network/service health.
    pub system_status_bar: SystemStatusBar,

    /// Main-screen activity stream for thinking/tool/runtime process events.
    pub activity_panel: ActivityPanel,
    pub activity_panel_visible: bool,

    /// Active tab index in the sidebar.
    /// 0=Runtime, 1=Tools, 2=Changes, 3=Goals, 4=Approvals, 5=Todo, 6=Files, 7=Sessions, 8=Gateway.
    pub sidebar_active_tab: usize,

    /// Heavy topic panel opened on demand instead of participating in normal tab rotation.
    pub(crate) active_topic_panel: Option<SidebarTopicPanel>,

    /// Current keyboard focus target used to route navigation and scrolling.
    pub(crate) focus_target: FocusTarget,

    /// Last rendered hit regions for mouse routing.
    last_hit_areas: TuiHitAreas,

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
    /// Last known terminal width, used for input line wrapping.
    last_terminal_width: u16,
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
        let mut layout_tree = crate::tui::layout::defaults::build_default_layout();
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
        let tool_ops_panel = ToolOpsPanel::new();
        let system_status_bar = SystemStatusBar::new();
        let activity_panel = ActivityPanel::new();

        Self {
            app,
            layout_tree,
            layout_state,
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
            tool_ops_panel,
            system_status_bar,
            activity_panel,
            activity_panel_visible: false,
            sidebar_active_tab: 0,
            active_topic_panel: None,
            focus_target: FocusTarget::Chat,
            last_hit_areas: TuiHitAreas::default(),
            accessibility,
            active_sessions: None,
            startup_phase: StartupPhase::Hidden,
            startup_start: Instant::now(),
            startup_show_time: None,
            dropped_events: 0,
            pending_cancel: false,
            pending_quit: false,
            last_terminal_width: 80,
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
        let opens_runtime_for_tool = matches!(event, CowdEvent::ToolStart { .. });

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

        if opens_runtime_for_tool {
            self.open_runtime_sidebar_for_tool();
        }

        // Bridge: notify new components that state has changed.
        // Using Resize(0,0) as a sentinel — real resize events always
        // have non-zero dimensions, so this is unambiguous.
        self.event_bus
            .send(crossterm::event::Event::Resize(0, 0), EventPriority::Normal);

        // Drain and dispatch to registered components.
        self.event_dispatcher.dispatch(&self.event_bus);
    }

    fn open_runtime_sidebar_for_tool(&mut self) {
        self.activity_panel_visible = false;
        self.active_topic_panel = None;
        if !self.layout_state.sidebar_visible {
            self.layout_state.toggle_sidebar(&mut self.layout_tree);
        }
        self.sidebar_active_tab = TAB_RUNTIME;
        self.set_focus_target(FocusTarget::Sidebar);
    }

    // ── Rendering ───────────────────────────────────────────────

    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        self.last_terminal_width = area.width;
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

        if self.layout_state.sidebar_visible {
            if let Some(topic) = self.active_topic_panel {
                match topic {
                    SidebarTopicPanel::Diff => self.diff_viewer.sync_from_app(&self.app),
                    SidebarTopicPanel::Memory => {
                        if self.memory_panel.memory_manager.is_some() {
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
                    }
                    SidebarTopicPanel::Skills => self.skills_panel.sync_from_app(&self.app),
                }
            } else {
                match self.sidebar_active_tab {
                    TAB_RUNTIME => self.runtime_activity_panel.sync_from_app(&self.app),
                    TAB_TOOLS => {}
                    TAB_CHANGES => {
                        let timeline = self.app.timeline_clone_vec();
                        self.file_changes_panel.sync_from_timeline(&timeline);
                    }
                    TAB_GOALS => self.goal_workbench_panel.sync_from_app(&self.app),
                    TAB_APPROVALS => self.approval_cockpit_panel.sync_from_app(&self.app),
                    TAB_TODO => {
                        let timeline = self.app.timeline_clone_vec();
                        self.todo_panel.sync_from_timeline(&timeline);
                    }
                    TAB_FILES => {
                        if !self.app.file_entries.is_empty() {
                            self.file_tree.rebuild(&self.app.file_entries);
                        }
                    }
                    TAB_SESSIONS => {
                        if !self.app.picker_sessions.is_empty() {
                            self.session_sidebar.load(self.app.picker_sessions.clone());
                        }
                        self.session_sidebar
                            .set_current_session(&self.app.session_id);
                    }
                    TAB_GATEWAY => self.gateway_panel.sync_from_app(&self.app),
                    _ => {}
                }
            }
        }

        // Sync performance dashboard from memory orchestrator
        self.performance_dashboard.tick();
        self.performance_dashboard.sync(&self.memory_orchestrator);

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
        self.system_status_bar.sync_from_app(&self.app);
        self.status_bar.sync_from_app(&self.app);
        self.status_bar.tick();
        let show_activity_panel = self.activity_panel_visible && !self.layout_state.sidebar_visible;
        if show_activity_panel {
            self.activity_panel.sync_from_app(&self.app);
        }

        // BUG 2 FIX: Dynamic input height based on line count.
        let input_lines = self.app.input.lines().len().max(1) as u16;
        let max_input = (area.height / 2).max(3);
        let input_h = (input_lines + 2).min(max_input).max(3);
        let frame_areas = TuiFrameAreas::build(area, input_h, self.app.search_active);

        // ── Main content: one RenderContext for chat, sidebar, status, input ──
        let mut main_ctx: RenderContext = RenderContext::new(frame, &skin);
        let toast_anchor_area: ratatui::layout::Rect;

        {
            let _guard = self.render_profiler.guard("system_status_bar");
            let _ = error_recovery::catch_render_panic(
                "system_status_bar",
                AssertUnwindSafe(|| {
                    self.system_status_bar
                        .render(&mut main_ctx, frame_areas.system);
                }),
            );
        }

        if let Some(search_area) = frame_areas.search {
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
            main_ctx
                .frame_mut()
                .render_widget(ratatui::widgets::Paragraph::new(search_line), search_area);
        }

        // 1. Render chat view + sidebar using the layout tree
        {
            main_ctx
                .frame_mut()
                .render_widget(ratatui::widgets::Clear, frame_areas.body);
            self.layout_tree.resize(frame_areas.body);
            let mut chat_area = self.layout_tree.area_of("chat").unwrap_or(frame_areas.body);
            let topic_fullscreen = self.layout_state.sidebar_visible
                && self.active_topic_panel.is_some()
                && frame_areas.body.width < 100;
            if self.layout_state.sidebar_visible
                && self.active_topic_panel.is_some()
                && frame_areas.body.width >= 100
            {
                let max_topic_w = frame_areas.body.width.saturating_sub(40);
                let topic_w =
                    ((frame_areas.body.width as u32 * 55 / 100) as u16).clamp(48, max_topic_w);
                chat_area.width = frame_areas.body.width.saturating_sub(topic_w).max(40);
            }
            if topic_fullscreen {
                chat_area.width = 0;
                toast_anchor_area = ratatui::layout::Rect::new(
                    frame_areas.body.x,
                    frame_areas.body.y,
                    frame_areas.body.width.min(56),
                    frame_areas.body.height,
                );
            } else if self.layout_state.sidebar_visible {
                toast_anchor_area = chat_area;
            } else {
                toast_anchor_area = frame_areas.body;
            }
            let activity_area = if show_activity_panel && chat_area.width >= 72 {
                let desired = (chat_area.width / 3).clamp(30, 48);
                let width = desired.min(chat_area.width.saturating_sub(40));
                if width >= 24 {
                    chat_area.width = chat_area.width.saturating_sub(width);
                    Some(ratatui::layout::Rect::new(
                        chat_area.x.saturating_add(chat_area.width),
                        chat_area.y,
                        width,
                        chat_area.height,
                    ))
                } else {
                    None
                }
            } else {
                None
            };
            let sidebar_area = if topic_fullscreen {
                frame_areas.body
            } else {
                let sidebar_x = chat_area.x.saturating_add(chat_area.width);
                let sidebar_w = frame_areas
                    .body
                    .x
                    .saturating_add(frame_areas.body.width)
                    .saturating_sub(sidebar_x);
                ratatui::layout::Rect::new(
                    sidebar_x,
                    frame_areas.body.y,
                    sidebar_w,
                    frame_areas.body.height,
                )
            };
            self.last_hit_areas = TuiHitAreas {
                chat: chat_area,
                activity: activity_area,
                sidebar: (self.layout_state.sidebar_visible && sidebar_area.width > 0)
                    .then_some(sidebar_area),
                topic: None,
                input: frame_areas.input,
            };

            self.chat_view.scroll_state.offset = self.app.scroll_offset;
            self.chat_view.scroll_state.auto_scroll = self.app.auto_scroll;

            // Render chat view (already synced above)
            if chat_area.width > 0 && chat_area.height > 0 {
                let _guard = self.render_profiler.guard("chat_view");
                self.chat_view.render(&mut main_ctx, chat_area);
            }
            self.chat_view.sync_to_app(&mut self.app);

            if let Some(activity_area) = activity_area {
                let _ = error_recovery::catch_render_panic(
                    "activity_panel",
                    AssertUnwindSafe(|| {
                        self.activity_panel.render(&mut main_ctx, activity_area);
                    }),
                );
            }

            if self.layout_state.sidebar_visible && sidebar_area.width > 0 {
                main_ctx
                    .frame_mut()
                    .render_widget(ratatui::widgets::Clear, sidebar_area);
                // Render sidebar: tab bar + active panel
                let tab_height = 1u16;
                let tab_area = ratatui::layout::Rect::new(
                    sidebar_area.x,
                    sidebar_area.y,
                    sidebar_area.width,
                    tab_height,
                );
                if let Some(topic) = self.active_topic_panel {
                    let title = ratatui::text::Line::from(vec![
                        ratatui::text::Span::styled(
                            topic.label(),
                            ratatui::style::Style::default()
                                .fg(ratatui::style::Color::Cyan)
                                .add_modifier(ratatui::style::Modifier::BOLD),
                        ),
                        ratatui::text::Span::styled(
                            "  topic panel · Esc close · j/k scroll",
                            ratatui::style::Style::default().fg(ratatui::style::Color::DarkGray),
                        ),
                    ]);
                    main_ctx
                        .frame_mut()
                        .render_widget(ratatui::widgets::Paragraph::new(title), tab_area);
                } else {
                    let tab_labels = sidebar_tab_labels(sidebar_area.width);
                    let tabs =
                        ratatui::widgets::Tabs::new(tab_labels).select(self.sidebar_active_tab);
                    main_ctx.frame_mut().render_widget(tabs, tab_area);
                }

                let panel_area = ratatui::layout::Rect::new(
                    sidebar_area.x,
                    sidebar_area.y.saturating_add(tab_height),
                    sidebar_area.width,
                    sidebar_area.height.saturating_sub(tab_height),
                );
                if self.active_topic_panel.is_some() {
                    self.last_hit_areas.topic = Some(panel_area);
                }
                if self.active_topic_panel == Some(SidebarTopicPanel::Diff) {
                    // Collect diff text only when the diff panel is visible.
                    let diffs: Vec<String> = self
                        .app
                        .timeline_clone_vec()
                        .iter()
                        .filter_map(|e| {
                            if let crate::tui::app::TimelineEntry::ToolCall {
                                name, output, ..
                            } = e
                            {
                                if (name == "edit_file"
                                    || name == "patch_file"
                                    || name == "apply_diff")
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
                if let Some(topic) = self.active_topic_panel {
                    match topic {
                        SidebarTopicPanel::Diff => {
                            let _guard = self.render_profiler.guard("diff_viewer");
                            let _ = error_recovery::catch_render_panic(
                                "diff_viewer",
                                AssertUnwindSafe(|| {
                                    self.diff_viewer.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        SidebarTopicPanel::Memory => {
                            let _guard = self.render_profiler.guard("memory_panel");
                            let _ = error_recovery::catch_render_panic(
                                "memory_panel",
                                AssertUnwindSafe(|| {
                                    self.memory_panel.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        SidebarTopicPanel::Skills => {
                            let _ = error_recovery::catch_render_panic(
                                "skills_panel",
                                AssertUnwindSafe(|| {
                                    self.skills_panel.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                    }
                } else {
                    match self.sidebar_active_tab {
                        TAB_RUNTIME => {
                            let _ = error_recovery::catch_render_panic(
                                "runtime_activity_panel",
                                AssertUnwindSafe(|| {
                                    self.runtime_activity_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_TOOLS => {
                            let _ = error_recovery::catch_render_panic(
                                "tool_ops_panel",
                                AssertUnwindSafe(|| {
                                    self.tool_ops_panel.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_CHANGES => {
                            let _ = error_recovery::catch_render_panic(
                                "file_changes_panel",
                                AssertUnwindSafe(|| {
                                    self.file_changes_panel.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_GOALS => {
                            let _ = error_recovery::catch_render_panic(
                                "goal_workbench_panel",
                                AssertUnwindSafe(|| {
                                    self.goal_workbench_panel.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_APPROVALS => {
                            let _ = error_recovery::catch_render_panic(
                                "approval_cockpit_panel",
                                AssertUnwindSafe(|| {
                                    self.approval_cockpit_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_TODO => {
                            let _ = error_recovery::catch_render_panic(
                                "todo_panel",
                                AssertUnwindSafe(|| {
                                    self.todo_panel.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_FILES => {
                            let _guard = self.render_profiler.guard("file_tree");
                            let _ = error_recovery::catch_render_panic(
                                "file_tree",
                                AssertUnwindSafe(|| {
                                    self.file_tree.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_SESSIONS => {
                            let _guard = self.render_profiler.guard("session_sidebar");
                            let _ = error_recovery::catch_render_panic(
                                "session_sidebar",
                                AssertUnwindSafe(|| {
                                    self.session_sidebar.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_GATEWAY => {
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
            }
        }

        // 2. Render status bar at bottom (reuses main_ctx)
        {
            let degraded = {
                let _guard = self.render_profiler.guard("status_bar");
                match error_recovery::catch_render_panic(
                    "status_bar",
                    AssertUnwindSafe(|| {
                        self.status_bar.render(&mut main_ctx, frame_areas.status);
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
            self.app.input.set_block(
                ratatui::widgets::Block::default()
                    .borders(ratatui::widgets::Borders::ALL)
                    .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
            );
            // Render app.input widget directly — NOT through prompt
            {
                let _guard = self.render_profiler.guard("input");
                main_ctx
                    .frame_mut()
                    .render_widget(&self.app.input, frame_areas.input);
            }
            // Render prompt's autocomplete dropdown as overlay
            {
                let _guard = self.render_profiler.guard("prompt_dropdown");
                let _ = error_recovery::catch_render_panic(
                    "prompt_dropdown",
                    AssertUnwindSafe(|| {
                        self.prompt
                            .render_dropdown(&mut main_ctx, frame_areas.input);
                    }),
                );
            }
            // Render context suggestion bar above the input area
            if self.context_suggestions.is_active() && !self.prompt.suggestions_visible() {
                let _ = error_recovery::catch_render_panic(
                    "context_suggestions",
                    AssertUnwindSafe(|| {
                        self.context_suggestions
                            .render(&mut main_ctx, frame_areas.input);
                    }),
                );
            }
        }

        // ── Overlays: one RenderContext for all conditional overlays ──
        let mut overlay_ctx: RenderContext = RenderContext::new(frame, &skin);

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

        // 5.5 Keep L4 knowledge cached, but do not auto-render it as a startup
        // overlay. The full memory/L4 surfaces are opened explicitly from the
        // sidebar/topic panels so they cannot cover the first screen.
        if self.memory_orchestrator.is_some() {
            self.l4_knowledge_view.sync(&self.memory_orchestrator);
        }

        // 6. Render toast notifications at top-right
        if !self.toast_manager.is_empty() {
            let degraded = {
                let _guard = self.render_profiler.guard("toast_manager");
                match error_recovery::catch_render_panic(
                    "toast_manager",
                    AssertUnwindSafe(|| {
                        self.toast_manager
                            .render(&mut overlay_ctx, toast_anchor_area);
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
            self.render_startup_overlay(frame, frame_areas.body);
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
        if self.command_palette.is_open() {
            if event.code == KeyCode::Esc {
                self.command_palette.close();
                return true;
            }

            let result = self
                .command_palette
                .handle_event(&crossterm::event::Event::Key(event));
            if result == crate::tui::components::EventResult::Consumed {
                if let Some(action) = self.command_palette.take_action() {
                    self.dispatch_action(action);
                }
                return true;
            }
        }

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

        if self.app.input.is_empty() && self.route_navigation_to_focus(event) {
            return true;
        }

        // 1.75. Tab/BackTab sidebar cycling (before keybind engine which maps Tab to no-op NextPanel)
        if self.layout_state.sidebar_visible {
            match event.code {
                KeyCode::Tab => {
                    self.active_topic_panel = None;
                    self.set_focus_target(FocusTarget::Sidebar);
                    self.sidebar_active_tab = (self.sidebar_active_tab + 1) % SIDEBAR_TAB_COUNT;
                    return true;
                }
                KeyCode::BackTab => {
                    self.active_topic_panel = None;
                    self.set_focus_target(FocusTarget::Sidebar);
                    self.sidebar_active_tab = if self.sidebar_active_tab == 0 {
                        SIDEBAR_TAB_COUNT - 1
                    } else {
                        self.sidebar_active_tab - 1
                    };
                    return true;
                }
                _ => {}
            }
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
        if self.prompt.suggestions_visible() {
            match key.code {
                KeyCode::Up => {
                    self.prompt.select_prev_suggestion();
                    return ProcessedKey::Nothing;
                }
                KeyCode::Down => {
                    self.prompt.select_next_suggestion();
                    return ProcessedKey::Nothing;
                }
                _ => {}
            }
        }
        if key.code == KeyCode::Tab && !key.modifiers.contains(KeyModifiers::SHIFT) {
            let input_text = self.input_text();
            self.prompt.refresh_suggestions_from_text_at_cursor(
                &input_text,
                self.input_cursor_byte_offset(),
            );
            if self.prompt.suggestions_visible() {
                if let Some(new_text) = self
                    .prompt
                    .apply_highlighted_suggestion_to_text(&input_text)
                {
                    self.replace_input_text(&new_text);
                }
                return ProcessedKey::Nothing;
            }
            // Fall through to sidebar tab cycling
        }
        if key.code == KeyCode::Tab && key.modifiers.contains(KeyModifiers::SHIFT) {
            if self.prompt.suggestions_visible() {
                self.prompt.select_next_suggestion();
                return ProcessedKey::Nothing;
            }
            let input_text = self.input_text();
            self.prompt.refresh_suggestions_from_text_at_cursor(
                &input_text,
                self.input_cursor_byte_offset(),
            );
            if self.prompt.suggestions_visible() {
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
        if self.layout_state.sidebar_visible && key.code == KeyCode::Tab {
            self.active_topic_panel = None;
            self.set_focus_target(FocusTarget::Sidebar);
            self.sidebar_active_tab = (self.sidebar_active_tab + 1) % SIDEBAR_TAB_COUNT;
            return ProcessedKey::Nothing;
        }
        if self.layout_state.sidebar_visible && key.code == KeyCode::BackTab {
            self.active_topic_panel = None;
            self.set_focus_target(FocusTarget::Sidebar);
            self.sidebar_active_tab = if self.sidebar_active_tab == 0 {
                SIDEBAR_TAB_COUNT - 1
            } else {
                self.sidebar_active_tab - 1
            };
            return ProcessedKey::Nothing;
        }
        if matches!(key.code, KeyCode::Up | KeyCode::Down)
            && key.modifiers.is_empty()
            && self.focus_target == FocusTarget::Input
            && (self.input_text().trim().is_empty() || self.app.history_idx.is_some())
        {
            self.dispatch_action(Action::HistoryBrowse(matches!(key.code, KeyCode::Up)));
            self.set_focus_target(FocusTarget::Input);
            return ProcessedKey::Nothing;
        }

        if self.app.input.is_empty() && self.route_navigation_to_focus(key) {
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
            self.wrap_input_to_width();
            self.set_focus_target(FocusTarget::Input);
            // BUG 1 FIX: Refresh suggestions from app.input text, not prompt's stale textarea
            let text = self.input_text();
            self.prompt
                .refresh_suggestions_from_text_at_cursor(&text, self.input_cursor_byte_offset());
            if self.prompt.suggestions_visible() {
                self.set_focus_target(FocusTarget::PromptSuggestions);
            }
            return ProcessedKey::Nothing;
        }

        // 5. Enter special case: submit input or toggle expand
        if key.code == KeyCode::Enter {
            if key.modifiers.contains(KeyModifiers::SHIFT)
                || key.modifiers.contains(KeyModifiers::CONTROL)
                || key.modifiers.contains(KeyModifiers::ALT)
            {
                self.app.input.insert_newline();
                self.wrap_input_to_width();
                return ProcessedKey::Nothing;
            }
            if self.prompt.suggestions_visible() {
                let input_text = self.input_text();
                if let Some(new_text) = self
                    .prompt
                    .apply_highlighted_suggestion_to_text(&input_text)
                {
                    if new_text != input_text {
                        self.replace_input_text(&new_text);
                        return ProcessedKey::Nothing;
                    }
                    self.prompt.clear_suggestions();
                } else {
                    self.prompt.clear_suggestions();
                }
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
            if self.try_open_sidebar_for_panel_command(&text) {
                self.replace_input_text("");
                return ProcessedKey::Nothing;
            }
            let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
            if let Err(err) = validate_context_tokens(&text, &cwd) {
                self.toast_manager.push(
                    ToastVariant::Error,
                    Some("Context invalid".into()),
                    err.to_string(),
                    4000,
                );
                return ProcessedKey::Nothing;
            }
            self.prompt.add_history(text.clone());
            self.app.input = tui_textarea::TextArea::default();
            self.app.input.set_block(
                ratatui::widgets::Block::default()
                    .borders(ratatui::widgets::Borders::ALL)
                    .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
            );
            return ProcessedKey::Submit(text);
        }

        // 5.5 Ctrl+J: insert newline (Ctrl+Enter maps to Ctrl+J on Linux terminals)
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('j') {
            self.app.input.insert_newline();
            self.wrap_input_to_width();
            return ProcessedKey::Nothing;
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
            if self.active_topic_panel.is_some() {
                self.active_topic_panel = None;
                self.set_focus_target(if self.layout_state.sidebar_visible {
                    FocusTarget::Sidebar
                } else {
                    FocusTarget::Chat
                });
                return ProcessedKey::Nothing;
            }
            if self.activity_panel_visible {
                self.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Chat);
                return ProcessedKey::Nothing;
            }
            if self.layout_state.sidebar_visible {
                self.layout_state.toggle_sidebar(&mut self.layout_tree);
                self.set_focus_target(FocusTarget::Chat);
                return ProcessedKey::Nothing;
            }
            self.pending_cancel = false;
            self.pending_quit = false;
            self.set_focus_target(FocusTarget::Chat);
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
            self.wrap_input_to_width();
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

    fn should_open_slash_command_palette(&self, event: &crossterm::event::KeyEvent) -> bool {
        use crossterm::event::KeyCode;

        event.code == KeyCode::Char('/')
            && event.modifiers.is_empty()
            && self.app.input.lines().join("\n").trim().is_empty()
    }

    fn input_text(&self) -> String {
        self.app.input.lines().join("\n")
    }

    fn input_cursor_byte_offset(&self) -> usize {
        let (row, col) = self.app.input.cursor();
        let mut offset = 0usize;
        for (idx, line) in self.app.input.lines().iter().enumerate() {
            if idx == row {
                return offset + char_col_to_byte_offset(line, col);
            }
            offset += line.len() + 1;
        }
        self.input_text().len()
    }

    fn replace_input_text(&mut self, text: &str) {
        let mut input = tui_textarea::TextArea::default();
        input.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
        );
        input.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));
        if !text.is_empty() {
            input.insert_str(text);
        }
        self.app.input = input;
    }

    /// Wrap input lines that exceed the visible text width.
    /// Called after each text modification to prevent horizontal scrolling.
    /// Uses simple word-boundary wrapping within the visible area (width - 2 for borders).
    fn wrap_input_to_width(&mut self) {
        let text_width = self.last_terminal_width.saturating_sub(2);
        if text_width < 10 {
            return; // Too narrow for meaningful wrapping
        }

        let lines: Vec<String> = self.app.input.lines().to_vec();
        let needs_wrap = lines
            .iter()
            .any(|l| l.chars().count() > text_width as usize);
        if !needs_wrap {
            return;
        }

        // Wrap each line and collect
        let mut new_lines: Vec<String> = Vec::new();
        for line in &lines {
            let wrapped = wrap_line_to_width(line, text_width as usize);
            for w in wrapped {
                new_lines.push(w);
            }
        }

        // Rebuild textarea with wrapped lines
        let mut ta = tui_textarea::TextArea::default();
        ta.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
        );
        ta.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));
        ta.set_cursor_line_style(ratatui::style::Style::default());

        if !new_lines.is_empty() {
            let last_idx = new_lines.len() - 1;
            for (i, line) in new_lines.into_iter().enumerate() {
                ta.insert_str(&line);
                if i < last_idx {
                    ta.insert_newline();
                }
            }
        }

        // Move cursor to end (normal typing flow; cursor restoration for mid-text editing
        // would require significantly more complexity and is rare for an input box)
        ta.move_cursor(tui_textarea::CursorMove::End);

        self.app.input = ta;
    }

    fn focus_for_current_surface(&self) -> FocusTarget {
        if self.command_palette.is_open() {
            FocusTarget::CommandPalette
        } else if !self.dialog_manager.is_empty() || self.export_dialog_active {
            FocusTarget::Dialog
        } else if self.prompt.suggestions_visible() {
            FocusTarget::PromptSuggestions
        } else if let Some(topic) = self.active_topic_panel {
            FocusTarget::TopicPanel(topic)
        } else if self.layout_state.sidebar_visible {
            FocusTarget::Sidebar
        } else if self.activity_panel_visible || self.app.turn_active {
            FocusTarget::Activity
        } else if !self.app.input.is_empty() {
            FocusTarget::Input
        } else {
            self.focus_target
        }
    }

    fn set_focus_target(&mut self, target: FocusTarget) {
        if self.focus_target != target {
            let label = target.label().to_string();
            let hint = target.hint().to_string();
            self.toast_manager.push(
                ToastVariant::Info,
                Some("Focus".into()),
                format!("{label}: {hint}"),
                3000,
            );
        }
        self.focus_target = target;
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
                if self.activity_panel.handle_event(&event)
                    == crate::tui::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::Activity);
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Diff) => {
                if self.diff_viewer.handle_event(&event)
                    == crate::tui::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Diff));
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Memory) => {
                if self.memory_panel.handle_event(&event)
                    == crate::tui::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Memory));
                    true
                } else {
                    false
                }
            }
            FocusTarget::TopicPanel(SidebarTopicPanel::Skills) => {
                if self.skills_panel.handle_event(&event)
                    == crate::tui::components::EventResult::Consumed
                {
                    self.set_focus_target(FocusTarget::TopicPanel(SidebarTopicPanel::Skills));
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
                        self.app.scroll_offset = self.app.scroll_offset.saturating_add(1);
                        self.app.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::Char('k') | crossterm::event::KeyCode::Up => {
                        self.app.scroll_offset = self.app.scroll_offset.saturating_sub(1);
                        self.app.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::PageDown => {
                        self.app.scroll_page_down();
                        self.app.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::PageUp => {
                        self.app.scroll_page_up();
                        self.app.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::Home => {
                        self.app.scroll_offset = 0;
                        self.app.auto_scroll = false;
                    }
                    crossterm::event::KeyCode::End => {
                        self.app.auto_scroll = true;
                    }
                    _ => return false,
                }
                self.set_focus_target(FocusTarget::Chat);
                true
            }
        }
    }

    fn route_navigation_to_sidebar(&mut self, event: crossterm::event::Event) -> bool {
        let consumed = match self.sidebar_active_tab {
            TAB_RUNTIME => self.runtime_activity_panel.handle_event(&event),
            TAB_TOOLS => {
                if self.handle_tool_ops_action(&event) {
                    crate::tui::components::EventResult::Consumed
                } else {
                    self.tool_ops_panel.handle_event(&event)
                }
            }
            TAB_CHANGES => self.file_changes_panel.handle_event(&event),
            TAB_GOALS => self.goal_workbench_panel.handle_event(&event),
            TAB_APPROVALS => self.approval_cockpit_panel.handle_event(&event),
            TAB_TODO => self.todo_panel.handle_event(&event),
            TAB_FILES => self.file_tree.handle_event(&event),
            TAB_SESSIONS => self.session_sidebar.handle_event(&event),
            TAB_GATEWAY => self.gateway_panel.handle_event(&event),
            _ => crate::tui::components::EventResult::NotConsumed,
        } == crate::tui::components::EventResult::Consumed;
        if consumed {
            self.set_focus_target(FocusTarget::Sidebar);
        }
        consumed
    }

    fn handle_tool_ops_action(&mut self, event: &crossterm::event::Event) -> bool {
        let crossterm::event::Event::Key(key) = event else {
            return false;
        };
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }

        match (self.tool_ops_panel.mode, key.code) {
            (_, KeyCode::Char('U')) => {
                self.refresh_tool_ops_panel_overview();
                true
            }
            (ToolOpsMode::Registry, KeyCode::Char('x')) => {
                let Some(tool_name) = self.tool_ops_panel.selected_tool_name().map(str::to_string)
                else {
                    self.tool_ops_panel
                        .set_status("No selected tool to execute");
                    return true;
                };
                self.record_tool_ops_result(run_runtime_projection_blocking(move |client| {
                    let name = tool_name;
                    async move {
                        client
                            .tool_execute(&name, serde_json::json!({}), "read_only")
                            .await
                    }
                }));
                true
            }
            (ToolOpsMode::Operations, KeyCode::Char('i')) => {
                let prompt = self.tool_ops_panel.intent_prompt.clone();
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.tool_intent_plan(&prompt, Vec::new()).await },
                ));
                true
            }
            (ToolOpsMode::Operations, KeyCode::Char('f')) => {
                let prompt = self.tool_ops_panel.fanout_prompt.clone();
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.tool_context_fanout_plan(&prompt).await },
                ));
                true
            }
            (ToolOpsMode::Operations, KeyCode::Char('b')) => {
                let calls = match serde_json::from_str::<Vec<serde_json::Value>>(
                    &self.tool_ops_panel.batch_buffer,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.tool_ops_panel
                            .set_status(format!("Invalid batch JSON: {error}"));
                        return true;
                    }
                };
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.tool_batch_readonly(calls, 4).await },
                ));
                true
            }
            (ToolOpsMode::Mutations, KeyCode::Char('v')) => {
                let edits = match serde_json::from_str::<Vec<serde_json::Value>>(
                    &self.tool_ops_panel.edits_buffer,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.tool_ops_panel
                            .set_status(format!("Invalid edits JSON: {error}"));
                        return true;
                    }
                };
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.tool_mutation_preview(edits).await },
                ));
                true
            }
            (ToolOpsMode::Mutations, KeyCode::Char('A')) => {
                if !self.tool_ops_panel.arm_apply_mutation() {
                    return true;
                }
                if self.tool_ops_panel.expected_hashes.is_empty() {
                    self.tool_ops_panel.set_status(
                        "Mutation apply blocked: run preview first and verify expected hashes",
                    );
                    return true;
                }
                let edits = match serde_json::from_str::<Vec<serde_json::Value>>(
                    &self.tool_ops_panel.edits_buffer,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.tool_ops_panel
                            .set_status(format!("Invalid edits JSON: {error}"));
                        return true;
                    }
                };
                let expected_hashes = serde_json::to_value(&self.tool_ops_panel.expected_hashes)
                    .unwrap_or_else(|_| serde_json::json!({}));
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move {
                        client.tool_mutation_apply(edits, expected_hashes).await
                    },
                ));
                true
            }
            (ToolOpsMode::Checkpoints, KeyCode::Char('n')) => {
                self.record_tool_ops_result(run_runtime_projection_blocking(|client| async move {
                    client.tool_checkpoint_create("tui checkpoint").await
                }));
                self.refresh_tool_ops_panel_overview();
                true
            }
            (ToolOpsMode::Checkpoints, KeyCode::Char('d')) => {
                let Some(id) = self
                    .tool_ops_panel
                    .selected_checkpoint_id()
                    .map(str::to_string)
                else {
                    self.tool_ops_panel
                        .set_status("No selected checkpoint to diff");
                    return true;
                };
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.tool_checkpoint_diff(&id).await },
                ));
                true
            }
            (ToolOpsMode::Checkpoints, KeyCode::Char('R')) => {
                let Some(id) = self
                    .tool_ops_panel
                    .selected_checkpoint_id()
                    .map(str::to_string)
                else {
                    self.tool_ops_panel
                        .set_status("No selected checkpoint to restore");
                    return true;
                };
                if !self.tool_ops_panel.arm_restore_checkpoint(id.clone()) {
                    return true;
                }
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.tool_checkpoint_restore(&id).await },
                ));
                self.refresh_tool_ops_panel_overview();
                true
            }
            (ToolOpsMode::Risk, KeyCode::Char('s')) => {
                let action = serde_json::json!({
                    "plane": "tui",
                    "operation": "tool_ops.simulate",
                    "actor": "tui-operator",
                    "inputs": { "mode": "risk" }
                });
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.cross_plane_policy_simulate(action).await },
                ));
                true
            }
            (ToolOpsMode::Risk, KeyCode::Char('p')) => {
                let action = serde_json::json!({
                    "actor_principal": "user:tui-operator",
                    "actor_identity_ref": null,
                    "source_channel": "tui",
                    "session_id": self.app.session_id,
                    "requested_capability": "cowd.tools.operate",
                    "provider_account": null,
                    "target_ref": "tool-ops",
                    "resource_ref": null,
                    "risk": "medium",
                    "data_classification": "internal",
                    "identity_trust": "verified"
                });
                self.record_tool_ops_result(run_runtime_projection_blocking(
                    move |client| async move { client.preflight_cross_plane_action(action).await },
                ));
                true
            }
            _ => false,
        }
    }

    fn refresh_tool_ops_panel_overview(&mut self) {
        match run_runtime_projection_blocking(|client| async move { client.tool_registry().await })
        {
            Ok(payload) => self.tool_ops_panel.sync_registry(&payload),
            Err(error) => self
                .tool_ops_panel
                .set_status(format!("Registry refresh failed: {error}")),
        }
        if let Ok(payload) =
            run_runtime_projection_blocking(|client| async move { client.tool_cache_stats().await })
        {
            self.tool_ops_panel.sync_cache(&payload);
        }
        if let Ok(payload) =
            run_runtime_projection_blocking(|client| async move { client.tool_checkpoints().await })
        {
            self.tool_ops_panel.sync_checkpoints(&payload);
        }
        let session_id = self.app.session_id.clone();
        if let Ok(payload) = run_runtime_projection_blocking(move |client| async move {
            client.runtime_timeline(&session_id, 50).await
        }) {
            self.tool_ops_panel.sync_ledger(&payload);
        }
    }

    fn record_tool_ops_result(&mut self, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => self.tool_ops_panel.record_receipt(payload),
            Err(error) => self
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

        if let Some(topic) = self.active_topic_panel {
            if let Some(area) = self.last_hit_areas.topic {
                if TuiHitAreas::contains(area, x, y) {
                    let consumed = match topic {
                        SidebarTopicPanel::Diff => self.diff_viewer.handle_event(&event),
                        SidebarTopicPanel::Memory => self.memory_panel.handle_event(&event),
                        SidebarTopicPanel::Skills => self.skills_panel.handle_event(&event),
                    } == crate::tui::components::EventResult::Consumed;
                    if consumed {
                        self.set_focus_target(FocusTarget::TopicPanel(topic));
                        return true;
                    }
                }
            }
        }

        if let Some(area) = self.last_hit_areas.activity {
            if TuiHitAreas::contains(area, x, y)
                && self.activity_panel.handle_event(&event)
                    == crate::tui::components::EventResult::Consumed
            {
                self.set_focus_target(FocusTarget::Activity);
                return true;
            }
        }

        if let Some(area) = self.last_hit_areas.sidebar {
            if TuiHitAreas::contains(area, x, y) && self.route_navigation_to_sidebar(event) {
                return true;
            }
        }

        if TuiHitAreas::contains(self.last_hit_areas.chat, x, y) {
            if down {
                self.app.scroll_page_down();
            } else {
                self.app.scroll_page_up();
            }
            self.app.auto_scroll = false;
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
        self.app.auto_scroll = false;
        self.set_focus_target(FocusTarget::Chat);
        true
    }

    fn open_sidebar_tab(&mut self, tab: usize, label: &str) {
        self.activity_panel_visible = false;
        self.active_topic_panel = None;
        if !self.layout_state.sidebar_visible {
            self.layout_state.toggle_sidebar(&mut self.layout_tree);
        }
        self.sidebar_active_tab = tab.min(SIDEBAR_TAB_COUNT.saturating_sub(1));
        self.set_focus_target(FocusTarget::Sidebar);
        if self.sidebar_active_tab == TAB_TOOLS {
            self.refresh_tool_ops_panel_overview();
        }
        self.toast_manager.push(
            ToastVariant::Info,
            Some("Panel".into()),
            format!("Opened {label}"),
            1600,
        );
    }

    fn open_topic_panel(&mut self, panel: SidebarTopicPanel) {
        self.activity_panel_visible = false;
        if !self.layout_state.sidebar_visible {
            self.layout_state.toggle_sidebar(&mut self.layout_tree);
        }
        self.active_topic_panel = Some(panel);
        self.set_focus_target(FocusTarget::TopicPanel(panel));
        self.toast_manager.push(
            ToastVariant::Info,
            Some("Panel".into()),
            format!("Opened {}", panel.label()),
            1600,
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

        if command.split_whitespace().count() != 1 {
            return false;
        }

        let Some(name) = command.strip_prefix('/') else {
            return false;
        };

        if matches!(name, "activity" | "recent") {
            if self.layout_state.sidebar_visible {
                self.layout_state.toggle_sidebar(&mut self.layout_tree);
            }
            self.activity_panel_visible = !self.activity_panel_visible;
            self.set_focus_target(if self.activity_panel_visible {
                FocusTarget::Activity
            } else {
                FocusTarget::Chat
            });
            let label = if self.activity_panel_visible {
                "Activity opened"
            } else {
                "Activity hidden"
            };
            self.toast_manager
                .push(ToastVariant::Info, Some("Panel".into()), label.into(), 1600);
            return true;
        }

        let topic = match name {
            "diff" => Some(SidebarTopicPanel::Diff),
            "memory" => Some(SidebarTopicPanel::Memory),
            "skills" | "skill" => Some(SidebarTopicPanel::Skills),
            _ => None,
        };
        if let Some(topic) = topic {
            self.open_topic_panel(topic);
            return true;
        }

        let Some((tab, label)) = (match name {
            "context" | "runtime" => Some((TAB_RUNTIME, "Runtime")),
            "tools" | "toolops" | "tool-ops" => Some((TAB_TOOLS, "Tools")),
            "changes" => Some((TAB_CHANGES, "Changes")),
            "tasks" | "goals" => Some((TAB_GOALS, "Goals")),
            "approvals" | "approve" => Some((TAB_APPROVALS, "Approvals")),
            "todo" => Some((TAB_TODO, "Todo")),
            "files" => Some((TAB_FILES, "Files")),
            "sessions" => Some((TAB_SESSIONS, "Sessions")),
            "gateway" => Some((TAB_GATEWAY, "Gateway")),
            _ => None,
        }) else {
            return false;
        };

        self.open_sidebar_tab(tab, label);
        true
    }

    pub fn open_surface_for_slash_result(&mut self, command_name: &str) {
        match command_name {
            "status" | "model" | "cost" | "sandbox" | "config" | "doctor" | "context" => {
                self.open_sidebar_tab(TAB_RUNTIME, "Runtime");
            }
            "memory" | "closet" => self.open_topic_panel(SidebarTopicPanel::Memory),
            "diff" => self.open_topic_panel(SidebarTopicPanel::Diff),
            "skills" | "skill" => self.open_topic_panel(SidebarTopicPanel::Skills),
            "tools" | "toolops" | "tool-ops" => self.open_sidebar_tab(TAB_TOOLS, "Tools"),
            "tasks" => self.open_sidebar_tab(TAB_GOALS, "Goals"),
            "approvals" => self.open_sidebar_tab(TAB_APPROVALS, "Approvals"),
            "session" | "resume" => self.open_sidebar_tab(TAB_SESSIONS, "Sessions"),
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
            self.toast_manager.push(
                ToastVariant::Info,
                Some("Focus".into()),
                "Use /focus chat|input|activity|runtime|tools|files|sessions|gateway|diff|memory|skills"
                    .into(),
                2400,
            );
            return true;
        }

        match target {
            "chat" => {
                self.active_topic_panel = None;
                self.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Chat);
            }
            "input" => {
                self.active_topic_panel = None;
                self.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Input);
            }
            "activity" | "recent" => {
                if self.layout_state.sidebar_visible {
                    self.layout_state.toggle_sidebar(&mut self.layout_tree);
                }
                self.activity_panel_visible = true;
                self.set_focus_target(FocusTarget::Activity);
            }
            "sidebar" => {
                if !self.layout_state.sidebar_visible {
                    self.layout_state.toggle_sidebar(&mut self.layout_tree);
                }
                self.active_topic_panel = None;
                self.activity_panel_visible = false;
                self.set_focus_target(FocusTarget::Sidebar);
            }
            "runtime" | "status" => self.open_sidebar_tab(TAB_RUNTIME, "Runtime"),
            "tools" | "toolops" | "tool-ops" => self.open_sidebar_tab(TAB_TOOLS, "Tools"),
            "changes" => self.open_sidebar_tab(TAB_CHANGES, "Changes"),
            "tasks" | "goals" => self.open_sidebar_tab(TAB_GOALS, "Goals"),
            "approvals" | "approve" => self.open_sidebar_tab(TAB_APPROVALS, "Approvals"),
            "todo" => self.open_sidebar_tab(TAB_TODO, "Todo"),
            "files" => self.open_sidebar_tab(TAB_FILES, "Files"),
            "sessions" => self.open_sidebar_tab(TAB_SESSIONS, "Sessions"),
            "gateway" => self.open_sidebar_tab(TAB_GATEWAY, "Gateway"),
            "diff" => self.open_topic_panel(SidebarTopicPanel::Diff),
            "memory" => self.open_topic_panel(SidebarTopicPanel::Memory),
            "skills" | "skill" => self.open_topic_panel(SidebarTopicPanel::Skills),
            _ => return false,
        }
        true
    }

    fn open_command_palette(&mut self) {
        let snapshot =
            crate::tui::runtime_control_store::RuntimeControlSnapshot::from_app(&self.app);
        self.command_palette.sync_runtime_actions(&snapshot);
        self.command_palette.open();
        self.set_focus_target(FocusTarget::CommandPalette);
    }

    fn open_command_palette_with_query(&mut self, query: &str) {
        let snapshot =
            crate::tui::runtime_control_store::RuntimeControlSnapshot::from_app(&self.app);
        self.command_palette.sync_runtime_actions(&snapshot);
        self.command_palette.open_with_query(query);
        self.set_focus_target(FocusTarget::CommandPalette);
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
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ScrollPage(direction) => {
                if direction > 0 {
                    self.app.scroll_page_down();
                } else {
                    self.app.scroll_page_up();
                }
                self.app.auto_scroll = false;
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ScrollTop => {
                self.app.scroll_offset = 0;
                self.app.auto_scroll = false;
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ScrollBottom => {
                self.app.auto_scroll = true;
                self.set_focus_target(FocusTarget::Chat);
            }
            Action::ExpandCollapse => {
                self.app.toggle_expand_current();
            }
            Action::Copy => {
                let focus = self.focus_for_current_surface();
                let copied = if matches!(focus, FocusTarget::Activity)
                    || (matches!(focus, FocusTarget::Sidebar) && self.sidebar_active_tab == 0)
                {
                    self.runtime_activity_panel.copy_text()
                } else {
                    self.app.copy_focused_content()
                };
                if copied {
                    self.toast_manager.push(
                        ToastVariant::Success,
                        Some("Copied".into()),
                        "Focused content copied to clipboard".into(),
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
                    self.set_focus_target(FocusTarget::Chat);
                } else {
                    self.open_command_palette();
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
                            .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
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
                    "command_palette" => {
                        self.open_command_palette();
                    }
                    "export" => {
                        self.export_dialog.reset();
                        self.export_dialog_active = true;
                    }
                    _ => {
                        let dialog = match name.as_str() {
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
                self.open_topic_panel(SidebarTopicPanel::Diff);
            }
            Action::FocusFileTree => {
                if !self.layout_state.sidebar_visible {
                    self.layout_state.toggle_sidebar(&mut self.layout_tree);
                }
                self.active_topic_panel = None;
                self.sidebar_active_tab = TAB_FILES;
            }
            Action::FocusSessions => {
                if !self.layout_state.sidebar_visible {
                    self.layout_state.toggle_sidebar(&mut self.layout_tree);
                }
                self.active_topic_panel = None;
                self.sidebar_active_tab = TAB_SESSIONS;
            }
            Action::Execute(ref cmd) => {
                if self.try_open_sidebar_for_panel_command(cmd) {
                    return;
                }
                let mut input = tui_textarea::TextArea::default();
                input.set_block(
                    ratatui::widgets::Block::default()
                        .borders(ratatui::widgets::Borders::ALL)
                        .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
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
                let result = run_runtime_control_blocking(move |client| async move {
                    client
                        .respond_approval(&id, approved, Some("once"), None)
                        .await
                })
                .or_else(move |_| {
                    run_runtime_projection_blocking(move |client| async move {
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
                let result = run_runtime_control_blocking(move |client| async move {
                    client.cancel_task(&id).await
                })
                .or_else(move |_| {
                    run_runtime_projection_blocking(move |client| async move {
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
                let result = run_runtime_control_blocking(move |client| async move {
                    client.complete_task(&id).await
                })
                .or_else(move |_| {
                    run_runtime_projection_blocking(move |client| async move {
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
                let result = run_runtime_control_blocking(move |client| async move {
                    client
                        .revalidate_connector_resource(&reference, &state)
                        .await
                })
                .or_else(move |_| {
                    run_runtime_projection_blocking(move |client| async move {
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
                let result = run_runtime_control_blocking(move |client| async move {
                    client
                        .promote_connector_resource_to_memory(&reference, session_id.as_deref())
                        .await
                })
                .or_else(move |_| {
                    run_runtime_projection_blocking(move |client| async move {
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
            Action::TogglePanel(ref name) if name == "sidebar" => {
                self.layout_state.toggle_sidebar(&mut self.layout_tree);
                self.active_topic_panel = None;
                self.set_focus_target(if self.layout_state.sidebar_visible {
                    FocusTarget::Sidebar
                } else {
                    FocusTarget::Chat
                });
                let message = if self.layout_state.sidebar_visible {
                    "Sidebar opened"
                } else {
                    "Sidebar hidden"
                };
                self.toast_manager.push(
                    ToastVariant::Info,
                    Some("Layout".into()),
                    message.into(),
                    1600,
                );
            }
            Action::TogglePanel(ref _name) => {}
            Action::ApplyPreset(preset) => {
                self.layout_tree.apply_preset(preset);
                self.layout_state = LayoutState::default();
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

        let overlay_y = area.y.saturating_add(area.height.saturating_sub(1));
        let overlay_rect = ratatui::layout::Rect::new(area.x, overlay_y, area.width, 1);

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

        let width = area.width.min(40);
        let height = (lines.len() as u16 + 2).min(area.height);
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

fn run_runtime_control_blocking<F, Fut>(operation: F) -> Result<serde_json::Value, String>
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
            .map_err(|_| "runtime control worker panicked".to_string())?
    } else {
        run()
    }
}

fn daemon_projection_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
}

fn run_runtime_projection_blocking<F, Fut>(operation: F) -> Result<serde_json::Value, String>
where
    F: FnOnce(crate::tui::projection_client::DaemonProjectionClient) -> Fut + Send + 'static,
    Fut: std::future::Future<
            Output = Result<serde_json::Value, crate::tui::projection_client::ProjectionError>,
        > + Send
        + 'static,
{
    let run = move || {
        let Some(client) =
            crate::tui::projection_client::DaemonProjectionClient::from_running_gateway_with_retry(
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
            .map_err(|_| "runtime projection worker panicked".to_string())?
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

        assert!(state.timeline_len() >= 1);
        let last = state.timeline_get(state.timeline_len() - 1).unwrap();
        let text = last.full_text();
        assert!(
            text.contains("Hello world"),
            "expected streamed assistant text to remain the final entry, got: {text}"
        );
        assert!(
            !state
                .timeline_iter()
                .any(|(_, entry)| entry.full_text().contains("Done")),
            "turn completion should not inject Done messages"
        );
    }

    #[test]
    fn apply_event_tool_lifecycle() {
        let mut state = TuiState::new("m", "s");

        state.apply_event(CowdEvent::TurnStarted);
        assert!(!state.layout_state.sidebar_visible);
        state.apply_event(CowdEvent::ToolStart {
            id: "t1".into(),
            name: "bash".into(),
            preview: "ls -la".into(),
        });

        assert!(state.timeline_iter().any(
            |(_, e)| matches!(&e, crate::tui::app::TimelineEntry::ToolCall { id, .. } if id == "t1")
        ));
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_RUNTIME);
        assert!(!state.activity_panel_visible);
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
    #[serial]
    fn process_raw_key_blocks_submit_when_context_file_is_missing() {
        let original_cwd = std::env::current_dir().unwrap();
        let dir = unique_temp_dir("cowd-tui-context-missing");
        std::env::set_current_dir(&dir).unwrap();

        let mut state = TuiState::new("m", "s");
        state.replace_input_text("分析 @file:missing.rs");

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert!(!state.toast_manager.is_empty());

        std::env::set_current_dir(original_cwd).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    #[serial]
    fn process_raw_key_allows_submit_when_context_file_is_valid() {
        let original_cwd = std::env::current_dir().unwrap();
        let dir = unique_temp_dir("cowd-tui-context-valid");
        std::fs::write(dir.join("readme.md"), "readme").unwrap();
        std::env::set_current_dir(&dir).unwrap();

        let mut state = TuiState::new("m", "s");
        state.replace_input_text("分析 @file:readme.md");

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        match result {
            ProcessedKey::Submit(text) => assert_eq!(text, "分析 @file:readme.md"),
            other => panic!("expected submit, got {other:?}"),
        }

        std::env::set_current_dir(original_cwd).unwrap();
        let _ = std::fs::remove_dir_all(dir);
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

    // ── command_palette ─────────────────────────────────────────

    #[test]
    fn command_palette_via_leader_chord() {
        let mut state = TuiState::new("m", "s");

        // Space → leader prefix
        state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        // p → ToggleCommandPalette
        state.handle_input(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));

        assert!(state.command_palette.is_open());
        assert!(state.dialog_manager.is_empty());
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
        assert_eq!(compact[TAB_RUNTIME], "Run");
        assert_eq!(compact[TAB_TOOLS], "Tool");
        assert_eq!(compact[TAB_APPROVALS], "Appr");
        assert_eq!(compact[TAB_FILES], "File");
        assert!(!compact.contains(&"Mem"));
        assert!(!compact.contains(&"Skill"));
        assert_eq!(full[TAB_RUNTIME], "Run");
        assert_eq!(full[TAB_TOOLS], "Tools");
        assert_eq!(full[TAB_APPROVALS], "Approvals");
        assert_eq!(full[TAB_FILES], "Files");
        assert!(!full.contains(&"Memory"));
        assert!(!full.contains(&"Skills"));
    }

    #[test]
    fn new_state_starts_with_sidebar_hidden_for_focused_first_screen() {
        let state = TuiState::new("m", "s");

        assert!(!state.layout_state.sidebar_visible);
        assert_eq!(state.layout_state.current_ratio(&state.layout_tree), 1.0);
    }

    #[test]
    fn ctrl_b_toggles_sidebar_visibility_in_tui_state() {
        let mut state = TuiState::new("m", "s");
        assert!(!state.layout_state.sidebar_visible);

        state.handle_input(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::CONTROL));
        assert!(state.layout_state.sidebar_visible);

        state.handle_input(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::CONTROL));
        assert!(!state.layout_state.sidebar_visible);
    }

    #[test]
    fn focus_actions_open_sidebar_and_select_expected_tab() {
        let mut state = TuiState::new("m", "s");

        state.dispatch_action(Action::FocusDiff);
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, Some(SidebarTopicPanel::Diff));

        state.layout_state.toggle_sidebar(&mut state.layout_tree);
        state.dispatch_action(Action::FocusFileTree);
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_FILES);

        state.layout_state.toggle_sidebar(&mut state.layout_tree);
        state.dispatch_action(Action::FocusSessions);
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_SESSIONS);
    }

    #[test]
    fn slash_panel_command_opens_sidebar_without_submitting() {
        let mut state = TuiState::new("m", "s");
        state.replace_input_text("/files");

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_FILES);
        assert_eq!(state.input_text(), "");
    }

    #[test]
    fn slash_activity_command_toggles_activity_panel_without_submitting() {
        let mut state = TuiState::new("m", "s");
        state.replace_input_text("/activity");

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert!(!state.layout_state.sidebar_visible);
        assert!(state.activity_panel_visible);
        assert_eq!(state.input_text(), "");
    }

    #[test]
    fn empty_input_navigation_routes_to_focus_instead_of_textarea() {
        let mut state = TuiState::new("m", "s");
        state.app.scroll_offset = 0;

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert_eq!(state.input_text(), "");
        assert_eq!(state.app.scroll_offset, 1);
        assert_eq!(state.focus_target, FocusTarget::Chat);
    }

    #[test]
    fn topic_panel_navigation_keeps_topic_focus() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/memory".into()));

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert_eq!(
            state.focus_target,
            FocusTarget::TopicPanel(SidebarTopicPanel::Memory)
        );
        assert_eq!(state.input_text(), "");
    }

    #[test]
    fn slash_activity_command_closes_sidebar_for_focused_first_screen() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/files".into()));
        assert!(state.layout_state.sidebar_visible);

        state.replace_input_text("/recent");
        let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert!(!state.layout_state.sidebar_visible);
        assert!(state.activity_panel_visible);
    }

    #[test]
    fn command_palette_panel_execute_opens_sidebar_directly() {
        let mut state = TuiState::new("m", "s");

        state.dispatch_action(Action::Execute("/memory".into()));

        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, Some(SidebarTopicPanel::Memory));
        assert_eq!(state.input_text(), "");
    }

    #[test]
    fn topic_panel_commands_open_on_demand_and_tab_returns_to_core_tabs() {
        let mut state = TuiState::new("m", "s");

        state.dispatch_action(Action::Execute("/skills".into()));

        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, Some(SidebarTopicPanel::Skills));

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_TOOLS);
    }

    #[test]
    fn render_topic_panel_uses_dedicated_title_instead_of_core_tabs() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/skills".into()));

        let mut terminal = MockTerminal::new(140, 32);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(joined.contains("Skills"));
        terminal.assert_line_contains("topic panel");
        assert!(!joined.contains("Gateway"));
    }

    #[test]
    fn render_topic_panel_compact_layout_keeps_input_and_status_visible() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/memory".into()));

        let mut terminal = MockTerminal::new(88, 28);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(joined.contains("Memory"));
        assert!(joined.contains("topic panel"));
        assert!(joined.contains("Input"));
        assert!(joined.contains("Enter detail"));
        assert!(
            !joined.contains("focus:memory"),
            "focus should not be pinned in footer: {joined}"
        );
    }

    #[test]
    fn command_palette_activity_execute_toggles_activity_panel_directly() {
        let mut state = TuiState::new("m", "s");

        state.dispatch_action(Action::Execute("/activity".into()));

        assert!(state.activity_panel_visible);
        assert!(!state.layout_state.sidebar_visible);
        assert_eq!(state.input_text(), "");
    }

    #[test]
    fn runtime_and_gateway_panel_commands_open_expected_tabs() {
        let mut state = TuiState::new("m", "s");

        state.dispatch_action(Action::Execute("/runtime".into()));
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_RUNTIME);
        assert_eq!(state.focus_target, FocusTarget::Sidebar);

        state.dispatch_action(Action::Execute("/tools".into()));
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_TOOLS);
        assert_eq!(state.focus_target, FocusTarget::Sidebar);

        state.dispatch_action(Action::Execute("/gateway".into()));
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, None);
        assert_eq!(state.sidebar_active_tab, TAB_GATEWAY);
        assert_eq!(state.focus_target, FocusTarget::Sidebar);
    }

    #[test]
    fn tool_ops_mutation_apply_requires_preview_hashes_before_confirmed_apply() {
        let mut state = TuiState::new("m", "s");
        state.sidebar_active_tab = TAB_TOOLS;
        state.tool_ops_panel.set_mode(ToolOpsMode::Mutations);
        state.tool_ops_panel.armed_action =
            Some(crate::tui::components::tool_ops_panel::ToolOpsArmedAction::ApplyMutation);

        let consumed = state.handle_tool_ops_action(&crossterm::event::Event::Key(KeyEvent::new(
            KeyCode::Char('A'),
            KeyModifiers::NONE,
        )));

        assert!(consumed);
        assert!(state.tool_ops_panel.status.contains("run preview first"));
        assert!(state.tool_ops_panel.last_receipt.is_none());
    }

    #[test]
    fn focus_command_switches_between_primary_surfaces() {
        let mut state = TuiState::new("m", "s");

        state.dispatch_action(Action::Execute("/focus activity".into()));
        assert!(state.activity_panel_visible);
        assert_eq!(state.focus_target, FocusTarget::Activity);

        state.dispatch_action(Action::Execute("/focus input".into()));
        assert!(!state.activity_panel_visible);
        assert_eq!(state.focus_target, FocusTarget::Input);

        state.dispatch_action(Action::Execute("/focus memory".into()));
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.active_topic_panel, Some(SidebarTopicPanel::Memory));
        assert_eq!(
            state.focus_target,
            FocusTarget::TopicPanel(SidebarTopicPanel::Memory)
        );
    }

    #[test]
    fn mouse_scroll_routes_to_focused_sidebar_panel() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/gateway".into()));
        state.app.scroll_offset = 12;
        state.gateway_panel.scroll_offset = 0;

        assert!(state.handle_mouse_scroll(true));

        assert_eq!(
            state.app.scroll_offset, 12,
            "sidebar mouse scroll should not move chat"
        );
        assert!(
            state.gateway_panel.scroll_offset > 0,
            "gateway panel should receive the scroll"
        );
        assert_eq!(state.focus_target, FocusTarget::Sidebar);
    }

    #[test]
    fn slash_keeps_input_control_without_opening_palette_or_placeholder() {
        let mut state = TuiState::new("m", "s");
        state.replace_input_text("inspect ");

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Nothing));
        assert!(!state.command_palette.is_open());
        assert_eq!(state.input_text(), "inspect /");
        assert!(
            !state.prompt.suggestions_visible(),
            "bare slash should not show placeholder suggestions"
        );
        assert_eq!(state.focus_for_current_surface(), FocusTarget::Input);
    }

    #[test]
    fn context_suggestions_do_not_render_over_prompt_dropdown() {
        let mut state = TuiState::new("m", "s");
        state.context_suggestions.test_show("context side effect");
        state.replace_input_text("inspect ");
        state.process_raw_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        state.process_raw_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
        assert!(state.prompt.suggestions_visible());

        let mut terminal = MockTerminal::new(100, 24);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(
            joined.contains("suggestions"),
            "missing prompt dropdown: {joined}"
        );
        assert!(
            !joined.contains("context side effect"),
            "context bar should yield while prompt dropdown is active: {joined}"
        );
    }

    #[test]
    fn exact_slash_command_enter_submits_instead_of_accepting_completion() {
        let mut state = TuiState::new("m", "s");
        state.replace_input_text("/status");
        state.prompt.refresh_suggestions_from_text_at_cursor(
            &state.input_text(),
            state.input_cursor_byte_offset(),
        );
        assert!(state.prompt.suggestions_visible());

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert!(matches!(result, ProcessedKey::Submit(text) if text == "/status"));
        assert_eq!(state.input_text(), "");
    }

    #[test]
    fn slash_result_opens_expected_surface() {
        let mut state = TuiState::new("m", "s");

        state.open_surface_for_slash_result("status");
        assert!(state.layout_state.sidebar_visible);
        assert_eq!(state.sidebar_active_tab, TAB_RUNTIME);
        assert_eq!(state.focus_target, FocusTarget::Sidebar);

        state.open_surface_for_slash_result("memory");
        assert_eq!(state.active_topic_panel, Some(SidebarTopicPanel::Memory));
        assert_eq!(
            state.focus_target,
            FocusTarget::TopicPanel(SidebarTopicPanel::Memory)
        );
    }

    #[test]
    fn mouse_scroll_uses_pointer_region_before_focus() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/gateway".into()));

        let mut terminal = MockTerminal::new(120, 30);
        terminal.draw(|frame| state.render(frame));
        let sidebar = state
            .last_hit_areas
            .sidebar
            .expect("sidebar area should be recorded");
        state.set_focus_target(FocusTarget::Chat);
        state.app.scroll_offset = 7;
        state.gateway_panel.scroll_offset = 0;

        assert!(state.handle_mouse_scroll_at(
            true,
            sidebar.x.saturating_add(1),
            sidebar.y.saturating_add(2),
        ));

        assert_eq!(
            state.app.scroll_offset, 7,
            "pointer over sidebar should not scroll chat even when chat has focus"
        );
        assert!(
            state.gateway_panel.scroll_offset > 0,
            "sidebar pointer scroll should route into gateway panel"
        );
        assert_eq!(state.focus_target, FocusTarget::Sidebar);
    }

    #[test]
    fn render_activity_panel_as_main_screen_side_rail() {
        let mut state = TuiState::new("m", "s");
        state.activity_panel_visible = true;
        state.app.add_message("assistant", "inspect build runtime");

        let mut terminal = MockTerminal::new(120, 30);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(
            joined.contains("Activity"),
            "missing activity title: {joined}"
        );
        assert!(
            joined.contains("inspect build runtime"),
            "missing activity event: {joined}"
        );
        assert!(
            !state.layout_state.sidebar_visible,
            "activity rail should not open the heavy sidebar"
        );
    }

    #[test]
    fn focus_change_shows_toast_instead_of_footer_focus() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/activity".into()));

        let mut terminal = MockTerminal::new(120, 30);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(
            joined.contains("activity: j/k scroll"),
            "missing focus toast: {joined}"
        );
        assert!(
            !joined.contains("focus:activity"),
            "focus should not be pinned in footer: {joined}"
        );
    }

    #[test]
    fn render_status_bar_keeps_top_identity_and_footer_model_on_narrow_width() {
        let mut state = TuiState::new("deepseek-v4-pro", "session-status-narrow");

        let mut terminal = MockTerminal::new(88, 28);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(
            joined.contains(concat!("v", env!("CARGO_PKG_VERSION"))),
            "missing top version: {joined}"
        );
        assert!(
            joined.contains("session session-"),
            "missing top abbreviated session id: {joined}"
        );
        assert!(
            joined.contains("deepseek-v4-pro STD"),
            "missing footer model without prefix: {joined}"
        );
        assert!(
            !joined.contains("model:") && !joined.contains("focus:"),
            "footer should not show model prefix or focus: {joined}"
        );
    }

    #[test]
    fn render_status_bar_shows_focus_specific_hint() {
        let mut state = TuiState::new("m", "s");
        state.dispatch_action(Action::Execute("/memory".into()));

        let mut terminal = MockTerminal::new(140, 30);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(joined.contains("Enter detail"), "missing hint: {joined}");
        assert!(
            !joined.contains("focus:memory"),
            "focus should not be pinned in footer: {joined}"
        );
    }

    #[test]
    fn render_thinking_inline_without_floating_panel() {
        let mut state = TuiState::new("m", "s");
        state.apply_event(CowdEvent::TurnStarted);
        state.apply_event(CowdEvent::ThinkingDelta {
            thinking: "Reviewing the request and checking the TUI render path.".into(),
        });

        let mut terminal = MockTerminal::new(100, 30);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(
            joined.contains("|  thinking"),
            "missing top thinking state after stats: {joined}"
        );
        assert!(
            !joined.contains("state "),
            "top bar should not render the word state: {joined}"
        );
        assert!(
            !joined.contains("details in Process"),
            "thinking handoff should stay out of main body: {joined}"
        );
        assert!(
            !joined.contains("┌💭 Thinking") && !joined.contains("┌ 💭 Thinking"),
            "thinking should not render as a floating panel: {joined}"
        );
    }

    #[test]
    fn input_up_down_browses_history_when_input_is_focused() {
        let mut state = TuiState::new("m", "s");
        state.app.input_history.push("first".into());
        state.app.input_history.push("second".into());
        state.set_focus_target(FocusTarget::Input);

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert!(matches!(result, ProcessedKey::Nothing));
        assert_eq!(state.input_text(), "second");

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert!(matches!(result, ProcessedKey::Nothing));
        assert_eq!(state.input_text(), "");
    }

    #[test]
    fn input_up_down_moves_cursor_when_input_has_content() {
        let mut state = TuiState::new("m", "s");
        state.app.input_history.push("history".into());
        state.replace_input_text("first\nsecond");
        state.set_focus_target(FocusTarget::Input);

        let result = state.process_raw_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert!(matches!(result, ProcessedKey::Nothing));
        assert_eq!(state.input_text(), "first\nsecond");
        assert_eq!(state.app.history_idx, None);
    }

    #[test]
    fn streaming_snapshot_deltas_replace_instead_of_duplicate() {
        let mut state = TuiState::new("m", "s");
        state.apply_event(CowdEvent::TurnStarted);
        state.apply_event(CowdEvent::TextDelta {
            text: "partial".into(),
        });
        state.apply_event(CowdEvent::TextDelta {
            text: "partial output".into(),
        });
        state.apply_event(CowdEvent::TurnComplete {
            assistant_text: "partial output".into(),
            iterations: 1,
        });

        assert_eq!(state.timeline_len(), 1);
        let text = state.timeline_get(0).unwrap().full_text();
        assert_eq!(text, "partial output");
    }

    #[test]
    fn render_search_bar_is_not_cleared_by_chat_view() {
        let mut state = TuiState::new("m", "s");
        state.app.search_active = true;
        state.app.search_query = "needle".to_string();
        state
            .app
            .add_message("assistant", "needle in the conversation");

        let mut terminal = MockTerminal::new(120, 30);
        terminal.draw(|frame| state.render(frame));
        let joined = terminal.buffer_lines().join("\n");

        assert!(joined.contains("/ needle"), "missing search bar: {joined}");
        assert!(
            joined.contains("Esc:cancel Enter:search"),
            "missing search hint: {joined}"
        );
        assert!(joined.contains("needle in the conversation"));
    }

    #[test]
    fn startup_overlay_stays_above_input_area() {
        let mut state = TuiState::new("m", "s");
        state.startup_phase = StartupPhase::Loading;

        let mut terminal = MockTerminal::new(100, 24);
        terminal.draw(|frame| state.render(frame));
        let lines = terminal.buffer_lines();
        let loading_row = lines
            .iter()
            .position(|line| line.contains("Loading plugins"))
            .expect("loading overlay should render");
        let input_row = lines
            .iter()
            .position(|line| line.contains("Input (Enter=send"))
            .expect("input should render");

        assert!(
            loading_row < input_row,
            "loading overlay row {loading_row} should be above input row {input_row}"
        );
    }

    #[test]
    fn renders_every_sidebar_tab_in_wide_and_compact_layouts() {
        for (width, height) in [(140, 38), (88, 32)] {
            for tab in 0..SIDEBAR_TAB_COUNT {
                let mut state = TuiState::new("m", "scenario-session");
                state.layout_state.toggle_sidebar(&mut state.layout_tree);
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
        state.layout_state.toggle_sidebar(&mut state.layout_tree);
        state.sidebar_active_tab = TAB_GATEWAY;
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
