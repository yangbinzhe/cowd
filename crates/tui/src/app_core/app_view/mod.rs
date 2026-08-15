//! Safe, declarative renderer and pure state machine for APP TUI documents.

mod render;
mod state;
mod stream;

pub use render::{render_app_view, AppViewRenderLimits};
pub use state::{AppViewInputResult, AppViewState, AppViewStateError, AppViewStateLimits};
pub use stream::{AppSubscriptionState, AppSubscriptionStatus, AppViewStreamState};

#[cfg(test)]
mod tests;
