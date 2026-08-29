//! Canonical lifecycle admission checks shared by every storage adapter.

use crate::{SessionError, SessionResult};

pub fn validate_plan_identity(
    operation_id: &str,
    session_id: &str,
    expected_generation: u64,
) -> SessionResult<()> {
    if operation_id.trim().is_empty() || session_id.trim().is_empty() || expected_generation == 0 {
        return Err(SessionError::InvalidArgument(
            "Session lifecycle plan requires non-empty identities and a positive generation"
                .to_string(),
        ));
    }
    Ok(())
}

pub fn validate_fence_metadata(
    actor: &str,
    reason: &str,
    transitional_status: &str,
) -> SessionResult<()> {
    if actor.trim().is_empty() || reason.trim().is_empty() || transitional_status.trim().is_empty()
    {
        return Err(SessionError::InvalidArgument(
            "Session lifecycle fence requires actor, reason, and transitional status".to_string(),
        ));
    }
    Ok(())
}
