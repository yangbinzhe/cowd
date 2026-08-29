use std::future::Future;
use std::pin::Pin;

use crate::state::TuiState;

pub(crate) type CoreGatewayFuture = Pin<
    Box<
        dyn Future<Output = Result<serde_json::Value, crate::gateway_client::GatewayApiError>>
            + Send,
    >,
>;
pub(crate) type CoreGatewayOperation =
    Box<dyn FnOnce(crate::gateway_client::GatewayApiClient) -> CoreGatewayFuture + Send>;
pub(crate) type CoreGatewayCompletion =
    Box<dyn FnOnce(&mut TuiState, Result<serde_json::Value, String>) + Send>;

pub(crate) struct PendingCoreGatewayEffect {
    pub session_id: String,
    pub authority_generation: u64,
    pub operation: CoreGatewayOperation,
    pub completion: CoreGatewayCompletion,
}

pub(crate) struct CompletedCoreGatewayEffect {
    session_id: String,
    authority_generation: u64,
    result: Result<serde_json::Value, String>,
    completion: CoreGatewayCompletion,
}

impl CompletedCoreGatewayEffect {
    pub(crate) fn new(
        session_id: String,
        authority_generation: u64,
        result: Result<serde_json::Value, String>,
        completion: CoreGatewayCompletion,
    ) -> Self {
        Self {
            session_id,
            authority_generation,
            result,
            completion,
        }
    }

    pub(crate) fn apply_if_current(self, state: &mut TuiState) {
        if crate::selectors::accepts_authority(state, &self.session_id, self.authority_generation) {
            (self.completion)(state, self.result);
        }
    }
}
