// ── TUI Component System ──────────────────────────────────────────
// Component trait, render context, event result, and component ID types.
// This is Wave 1, Task 1 — the foundation for all TUI components.

pub mod agents_overlay;
pub mod base;
pub mod chat_view;
pub mod command_palette;
pub mod context_panel;
pub mod dialog;
pub mod diff_viewer;
pub mod revert_dialog;
pub mod export_dialog;
pub mod file_changes_panel;
pub mod file_tree;
pub mod gateway_panel;
pub mod prompt;
pub mod render_engine;
pub mod session_sidebar;
pub mod status_bar;
pub mod thinking_panel;
pub mod memory_panel;
pub mod question_form;
pub mod toast;
pub mod skills_panel;
pub mod todo_panel;
pub mod session_events;

#[cfg(test)]
mod base_test;

pub use base::{Component, EventResult, RenderContext};
