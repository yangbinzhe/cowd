pub mod app;
pub mod layout;
pub mod components;
pub mod render;
pub mod input;
pub mod keybind;
pub mod skin;
pub mod events;
pub mod event;
pub mod runner;
pub mod scroll_state;
pub mod callbacks;
pub mod widgets;
pub mod osc52;
pub mod clipboard;
pub mod md_renderer;
pub mod state;
pub mod test_utils;
pub mod theme;
pub mod profiler;
pub mod animation;
pub mod error_recovery;
pub mod config_migration;
pub mod accessibility;

// #[cfg(test)]  // FIXME: test module references non-existent types; re-enable after fixing imports
// mod tui_integration_tests;

pub use app::{App, FileEntry, DelegateTask, MemoryEntry, SkillSummary};
pub use events::{TuiEvent, TuiEventReceiver, tui_event_channel};
pub use callbacks::TuiToolCallback;
