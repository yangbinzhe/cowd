use std::time::Instant;

use super::{FocusTarget, StartupPhase, TuiHitAreas};

pub struct ShellState {
    pub layout_tree: crate::layout::LayoutTree,
    pub layout_state: crate::layout::LayoutState,
    pub chat_view: crate::components::chat_view::ChatView,
    pub keybind_engine: crate::keybind::KeybindEngine,
    pub event_bus: crate::event::EventBus,
    pub event_dispatcher: crate::event::dispatcher::EventDispatcher,
    pub theme_engine: crate::theme::ThemeEngine,
    pub prompt: crate::components::prompt::Prompt,
    pub composer: crate::components::composer::Composer,
    pub(super) composer_content_width: u16,
    pub(super) composer_desired_column: Option<u16>,
    pub(crate) focus_target: FocusTarget,
    pub(super) last_hit_areas: TuiHitAreas,
    pub status_bar: crate::components::status_bar::StatusBar,
    pub animation_engine: crate::animation::AnimationEngine,
    pub frame_timer: crate::profiler::FrameTimer,
    pub render_profiler: crate::profiler::RenderProfiler,
    pub accessibility: crate::accessibility::AccessibilityMode,
    pub active_sessions: usize,
    pub startup_phase: StartupPhase,
    pub startup_start: Instant,
    pub startup_show_time: Option<Instant>,
    pub dropped_events: usize,
    pub(super) pending_cancel: bool,
    pub(super) pending_quit: bool,
    pub(super) last_terminal_width: u16,
}

impl super::TuiState {
    /// Set the active Gateway session count projected through the Gateway boundary.
    pub fn set_active_sessions_count(&mut self, active_sessions: usize) {
        crate::reducer::reduce(
            self,
            crate::action::UiAction::SetActiveSessions(active_sessions),
        );
    }

    /// Mark whether the Gateway memory projection is currently available.
    pub fn set_memory_projection_available(&mut self, available: bool) {
        crate::reducer::reduce(
            self,
            crate::action::UiAction::SetMemoryProjectionAvailable(available),
        );
    }
}
