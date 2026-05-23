// ── TUI Component System ──────────────────────────────────────────
// Component trait, render context, event result, and component ID types.
// This is Wave 1, Task 1 — the foundation for all TUI components.

pub mod base;
pub mod dialog;
pub mod render_engine;
pub mod prompt;

#[cfg(test)]
mod base_test;

pub use base::{Component, ComponentId, EventResult, RenderContext};
