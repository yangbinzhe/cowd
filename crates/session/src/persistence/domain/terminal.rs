//! Canonical terminal transcript and execution-fence validation.

use super::super::{SessionMessage, SessionTerminalTranscriptCommit};
use crate::{SessionError, SessionResult};

pub fn validate_terminal_transcript(
    terminal_message_id: &str,
    ingress_message_id: &str,
    session_id: &str,
    messages: &[SessionMessage],
) -> SessionResult<()> {
    if terminal_message_id.trim().is_empty()
        || ingress_message_id.trim().is_empty()
        || session_id.trim().is_empty()
        || messages.is_empty()
        || messages
            .last()
            .is_none_or(|message| message.stable_message_id != terminal_message_id)
    {
        return Err(SessionError::InvalidArgument(
            "terminal transcript requires a non-empty session, ingress, terminal ID, and final row"
                .to_string(),
        ));
    }
    if messages.iter().any(|message| {
        message.stable_message_id.trim().is_empty()
            || message.session_id != session_id
            || message.role.trim().is_empty()
            || serde_json::from_str::<serde_json::Value>(&message.content_json)
                .ok()
                .and_then(|value| value.as_array().cloned())
                .is_none()
    }) {
        return Err(SessionError::InvalidArgument(
            "terminal transcript contains an invalid message row".to_string(),
        ));
    }
    let unique_ids = messages
        .iter()
        .map(|message| message.stable_message_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if unique_ids.len() != messages.len() {
        return Err(SessionError::InvalidArgument(
            "terminal transcript contains duplicate stable message IDs".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_terminal_commit(request: &SessionTerminalTranscriptCommit) -> SessionResult<()> {
    if request.turn_id.trim().is_empty()
        || request.runtime_commit_cursor == 0
        || request.consumed_input_sequence < request.fence.input_sequence
        || request.fence.request_id.trim().is_empty()
        || request.fence.session_generation == 0
        || request.fence.claim_owner.trim().is_empty()
        || request.fence.claim_token.trim().is_empty()
        || request.fence.claim_fence_epoch == 0
    {
        return Err(SessionError::InvalidArgument(
            "terminal commit requires complete turn, cursor and live execution fence identity"
                .to_string(),
        ));
    }
    Ok(())
}
