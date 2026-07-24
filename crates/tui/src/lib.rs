#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

#[path = "rendering/accessibility.rs"]
pub mod accessibility;
#[path = "app_core/action_coverage.rs"]
pub mod action_coverage;
#[path = "rendering/animation.rs"]
pub mod animation;
#[path = "app_core/app.rs"]
pub mod app;
#[path = "app_core/app_surface_host.rs"]
pub mod app_surface_host;
#[path = "app_core/boundary_policy.rs"]
mod boundary_policy;
#[path = "platform/clipboard.rs"]
pub mod clipboard;
pub mod components;
#[path = "platform/config_migration.rs"]
pub mod config_migration;
#[path = "platform/context_tokens.rs"]
pub mod context_tokens;
#[path = "platform/error_recovery.rs"]
pub mod error_recovery;
pub mod event;
#[path = "app_core/events.rs"]
pub mod events;
#[path = "gateway/gateway_client.rs"]
pub mod gateway_client;
pub mod keybind;
pub mod layout;
#[path = "rendering/md_renderer.rs"]
pub mod md_renderer;
#[path = "platform/osc52.rs"]
pub mod osc52;
#[path = "rendering/profiler.rs"]
pub mod profiler;
#[path = "app_core/protocol.rs"]
pub mod protocol;
#[path = "rendering/render.rs"]
pub mod render;
#[path = "gateway/runner.rs"]
pub mod runner;
#[path = "app_core/runtime_control_store.rs"]
pub mod runtime_control_store;
#[path = "rendering/scroll_state.rs"]
pub mod scroll_state;
#[path = "rendering/skin.rs"]
pub mod skin;
#[path = "app_core/state.rs"]
pub mod state;
#[cfg(test)]
pub mod test_utils;
pub mod theme;
pub mod workbench;
#[path = "rendering/wrapping.rs"]
pub mod wrapping;

#[cfg(test)]
#[path = "integration/tui_integration_tests.rs"]
mod tui_integration_tests;

pub use app::{App, DelegateTask, FileEntry, MemoryEntry, SkillSummary};
pub use boundary_policy::{TuiBackendAccess, TuiBoundaryPolicy};
#[allow(unused_imports)]
pub use events::{cowd_event_channel, CowdEventReceiver};
pub use protocol::{CowdEvent, RuntimeExecutionGraphSummary, RuntimePolicyDecisionSummary};
pub use runner::{run_gateway_tui, terminal_entry, GatewayTuiConfig};
