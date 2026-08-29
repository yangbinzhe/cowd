use std::time::Instant;

use super::{PendingAppSurfaceCommand, PendingCoreGatewayEffect};

pub struct SessionUiState {
    pub memory_projection_available: bool,
    pub(super) memory_panel_last_sync: Option<Instant>,
    pub session_sidebar: crate::components::session_sidebar::SessionSidebar,
    pub app_surface_host: crate::app_surface_host::DeclarativeAppHost,
    pub(crate) pending_app_surface_commands: Vec<PendingAppSurfaceCommand>,
    pub(crate) pending_core_gateway_effects: Vec<PendingCoreGatewayEffect>,
    pub(crate) authority_generation: u64,
    pub(crate) authorization_revoked: bool,
    pub(crate) session_catalog_fingerprint: Option<u64>,
}
