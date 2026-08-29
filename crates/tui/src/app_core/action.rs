/// Synchronous mutations admitted by the terminal composition root.
pub(crate) enum UiAction {
    SetActiveSessions(usize),
    SetMemoryProjectionAvailable(bool),
    InstallSessionAuthority {
        generation: u64,
    },
    RevokeSessionAuthority {
        reason: String,
    },
    ApplySessionCatalog {
        fingerprint: u64,
        sessions: Vec<crate::app::SessionSummary>,
    },
}
