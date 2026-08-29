use crate::action::UiAction;
use crate::state::TuiState;

pub(crate) fn reduce(state: &mut TuiState, action: UiAction) -> bool {
    match action {
        UiAction::SetActiveSessions(active) => state.shell.active_sessions = active,
        UiAction::SetMemoryProjectionAvailable(available) => {
            state.session.memory_projection_available = available;
        }
        UiAction::InstallSessionAuthority { generation } => {
            state.session.authority_generation = generation.max(1);
            state.session.authorization_revoked = false;
            state.session.pending_app_surface_commands.clear();
            state.session.pending_core_gateway_effects.clear();
        }
        UiAction::RevokeSessionAuthority { reason } => {
            state.session.authority_generation =
                state.session.authority_generation.wrapping_add(1).max(1);
            state.session.authorization_revoked = true;
            state.session.pending_app_surface_commands.clear();
            state.session.pending_core_gateway_effects.clear();
            state.app.revoke_session_authorization(&reason);
        }
        UiAction::ApplySessionCatalog {
            fingerprint,
            sessions,
        } => {
            if state.session.session_catalog_fingerprint == Some(fingerprint) {
                return false;
            }
            state.session.session_catalog_fingerprint = Some(fingerprint);
            state.app.shell.picker_sessions = sessions.clone();
            state.session.session_sidebar.refresh_if_changed(sessions);
            state
                .session
                .session_sidebar
                .set_current_session(&state.app.shell.session_id);
        }
    }
    state.app.request_redraw();
    true
}
