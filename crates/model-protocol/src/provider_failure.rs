use serde::{Deserialize, Serialize};

/// Scope of the state that must change before a provider request can succeed.
///
/// This is intentionally orthogonal to request retryability. A request-scoped
/// failure may be retried under the governed retry policy, while account and
/// configuration failures require a different route or operator action.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureScope {
    #[default]
    Request,
    Account,
    Configuration,
}

impl ProviderFailureScope {
    #[must_use]
    pub const fn route_is_unavailable(self) -> bool {
        matches!(self, Self::Account | Self::Configuration)
    }
}
