// ── TUI Component System ──────────────────────────────────────────
// Component trait, render context, event result, and component ID types.
// This is Wave 1, Task 1 — the foundation for all TUI components.

pub mod base;
pub mod chat_view;
pub mod dialog;
pub mod diff_viewer;
pub mod export_dialog;
pub mod file_changes_panel;
pub mod file_tree;
pub mod render_engine;
pub mod prompt;
pub mod session_sidebar;
pub mod status_bar;
pub mod toast;
pub mod todo_panel;

#[cfg(test)]
mod base_test;

pub use base::{Component, ComponentId, EventResult, RenderContext};
