use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use surface::{SurfaceFrame, SurfaceResource, SurfaceRoute};
use tokio::process::Child;
use tokio::sync::Mutex as AsyncMutex;

use super::edge_h2::EdgeH2Client;

#[derive(Debug)]
pub(super) struct ManagedSurfaceProcess {
    pub(super) pid: Option<u32>,
    pub(super) started_at: DateTime<Utc>,
    pub(super) client: EdgeH2Client,
    pub(super) child: Arc<AsyncMutex<Child>>,
    pub(super) events: Arc<AsyncMutex<VecDeque<SurfaceFrame>>>,
    pub(super) runtime_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceDiscoveryReport {
    pub(crate) roots: Vec<String>,
    pub(crate) discovered: usize,
    #[serde(default)]
    pub(crate) removed: Vec<String>,
    pub(crate) failures: Vec<SurfaceDiscoveryFailure>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceDiscoveryFailure {
    pub(crate) path: String,
    pub(crate) error: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceHostHealth {
    pub(crate) status: String,
    pub(crate) surface_count: usize,
    pub(crate) external_surface_count: usize,
    pub(crate) route_count: usize,
    pub(crate) resource_count: usize,
    pub(crate) ready_count: usize,
    pub(crate) degraded_count: usize,
    pub(crate) failed_count: usize,
    pub(crate) circuit_open_count: usize,
    pub(crate) roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceStaticFile {
    pub(crate) surface: String,
    pub(crate) mount: String,
    pub(crate) requested_path: String,
    pub(crate) file_path: PathBuf,
    pub(crate) spa_fallback: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceRouteSummary {
    pub(crate) surface: String,
    pub(crate) routes: Vec<SurfaceRoute>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceResourceSummary {
    pub(crate) surface: String,
    pub(crate) resources: Vec<SurfaceResource>,
}
