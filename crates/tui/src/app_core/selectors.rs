use crate::state::TuiState;

pub(crate) fn accepts_authority(state: &TuiState, session_id: &str, generation: u64) -> bool {
    !state.session.authorization_revoked
        && state.app.shell.session_id == session_id
        && state.session.authority_generation == generation
}

pub(crate) fn authority_generation(state: &TuiState) -> u64 {
    state.session.authority_generation
}
