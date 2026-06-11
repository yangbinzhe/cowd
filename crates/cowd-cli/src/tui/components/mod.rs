// ── TUI Component System ──────────────────────────────────────────
// Component trait, render context, event result, and component ID types.
// This is Wave 1, Task 1 — the foundation for all TUI components.

pub mod activity_panel;
pub mod agent_team_panel;
pub mod agents_overlay;
pub mod approval_cockpit_panel;
pub mod base;
pub mod chat_view;
pub mod command_palette;
pub mod context_panel;
pub mod context_suggestions;
pub mod dialog;
pub mod diff_viewer;
pub mod discussion_thread_view;
pub mod export_dialog;
pub mod file_changes_panel;
pub mod file_tree;
pub mod gateway_panel;
pub mod goal_workbench_panel;
pub mod l4_knowledge_view;
pub mod memory_panel;
pub mod performance_dashboard;
pub mod prompt;
pub mod question_form;
pub mod render_engine;
pub mod revert_dialog;
pub mod runtime_activity_panel;
pub mod session_sidebar;
pub mod skills_panel;
pub mod status_bar;
pub mod system_status_bar;
pub mod task_decomposition_view;
pub mod thinking_panel;
pub mod toast;
pub mod todo_panel;

#[cfg(test)]
mod base_test;

pub use base::{Component, EventResult, RenderContext};
