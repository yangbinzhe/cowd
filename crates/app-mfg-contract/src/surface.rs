use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{mutation::MfgActionContract, route::MfgRouteContract, version::MfgContractVersion};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgSurfaceKind {
    Webui,
    Tui,
    Cli,
    Management,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgSurfaceRole {
    EnhancedManagement,
    ConsoleUnavailable,
    ConsoleReadOnly,
    ConsoleOperationalControl,
    MinimalCoreControl,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgSurfaceContract {
    pub surface: MfgSurfaceKind,
    pub role: MfgSurfaceRole,
    #[serde(default)]
    pub entrypoints: Vec<String>,
    #[serde(default)]
    pub routes: Vec<crate::route::MfgRouteId>,
    #[serde(default)]
    pub actions: Vec<crate::mutation::MfgActionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgFrontendContractV1 {
    pub kind: String,
    pub contract_version: MfgContractVersion,
    pub generated_at: DateTime<Utc>,
    pub app_id: String,
    pub active_route_count: usize,
    pub planned_route_count: usize,
    #[serde(default)]
    pub routes: Vec<MfgRouteContract>,
    #[serde(default)]
    pub actions: Vec<MfgActionContract>,
    #[serde(default)]
    pub surfaces: Vec<MfgSurfaceContract>,
    /// Effective capability grant for the authenticated principal on the
    /// requesting surface. It is descriptive and never authorizes a request;
    /// Gateway middleware remains the enforcement owner.
    #[serde(default)]
    pub granted_capabilities: Vec<String>,
}
