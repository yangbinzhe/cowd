pub mod accessibility;
pub mod animation;
pub mod app;
pub mod callbacks;
pub mod clipboard;
pub mod components;
pub mod config_migration;
pub mod context_tokens;
pub mod error_recovery;
pub mod event;
pub mod events;
pub mod gateway_client;
pub mod keybind;
pub mod layout;
pub mod md_renderer;
pub mod osc52;
pub mod profiler;
pub mod render;
pub mod runner;
pub mod runtime_control_store;
pub mod scroll_state;
pub mod skin;
pub mod state;
pub mod test_utils;
pub mod theme;

#[cfg(test)]
mod tui_integration_tests;

pub use app::{App, DelegateTask, FileEntry, MemoryEntry, SkillSummary};
pub use callbacks::{TuiMemoryCallback, TuiToolCallback};
#[allow(unused_imports)]
pub use events::{cowd_event_channel, CowdEventReceiver, TuiEvent};
