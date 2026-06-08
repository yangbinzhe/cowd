use crate::tui::app::App;
use crate::tui::control_client::{DaemonControlClient, DaemonSessionLease, DaemonStatus};
use crate::tui::projection_client::DaemonProjectionClient;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DaemonTaskSummary {
    pub id: String,
    pub objective: String,
    pub status: String,
    pub current_phase: Option<String>,
    pub yolo_mode: bool,
    pub failure_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeControlSnapshot {
    pub daemon_running: bool,
    pub active_sessions: usize,
    pub uptime_secs: Option<u64>,
    pub session_ids: Vec<String>,
    pub runtime_readiness: Option<String>,
    pub runtime_components: Option<u64>,
    pub task_count: Option<u64>,
    pub tasks: Vec<DaemonTaskSummary>,
    pub pending_approvals: Option<u64>,
    pub lease_owner: Option<String>,
    pub lease_mode: Option<String>,
    pub memory_status: Option<String>,
    pub cross_plane_grants_active: Option<u64>,
    pub cross_plane_actions_24h: Option<u64>,
    pub degraded_reasons: Vec<String>,
}

impl RuntimeControlSnapshot {
    pub fn from_status(status: &DaemonStatus) -> Self {
        Self {
            daemon_running: true,
            active_sessions: status.active_sessions,
            uptime_secs: Some(status.uptime_secs),
            ..Self::default()
        }
    }

    pub fn from_app(app: &App) -> Self {
        Self {
            daemon_running: app.server_running,
            active_sessions: app.active_api_sessions,
            uptime_secs: app.server_uptime_secs,
            runtime_readiness: app.daemon_runtime_readiness.clone(),
            runtime_components: app.daemon_runtime_components,
            task_count: app.daemon_task_count,
            tasks: app.daemon_tasks.clone(),
            pending_approvals: app.daemon_pending_approvals,
            lease_owner: app.daemon_lease_owner.clone(),
            lease_mode: app.daemon_lease_mode.clone(),
            ..Self::default()
        }
    }

    pub fn apply_lease(&mut self, lease: &DaemonSessionLease) {
        self.lease_owner = Some(lease.owner.clone());
        self.lease_mode = Some(lease.mode.clone());
    }

    pub fn apply_to_app(&self, app: &mut App) {
        app.server_running = self.daemon_running;
        app.server_uptime_secs = self.uptime_secs;
        app.active_api_sessions = self.active_sessions;
        app.daemon_runtime_readiness = self.runtime_readiness.clone();
        app.daemon_runtime_components = self.runtime_components;
        app.daemon_task_count = self.task_count;
        app.daemon_tasks = self.tasks.clone();
        app.daemon_pending_approvals = self.pending_approvals;
        app.daemon_lease_owner = self.lease_owner.clone();
        app.daemon_lease_mode = self.lease_mode.clone();
    }

    pub fn ingest_session_ids(&mut self, session_ids: Vec<String>) {
        self.active_sessions = session_ids.len();
        self.session_ids = session_ids;
    }

    pub fn ingest_runtime_control_plane(&mut self, value: &serde_json::Value) {
        self.runtime_readiness = value
            .pointer("/readiness/score")
            .or_else(|| value.pointer("/diagnostics/readiness_score"))
            .and_then(serde_json::Value::as_u64)
            .map(|score| format!("{score}%"))
            .or_else(|| Some("unknown".to_string()));
        self.runtime_components = value
            .pointer("/diagnostics/component_count")
            .and_then(serde_json::Value::as_u64);
    }

    pub fn ingest_task_status(&mut self, value: &serde_json::Value) {
        self.tasks = value
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .map(|tasks| {
                tasks
                    .iter()
                    .filter_map(task_summary_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.task_count = Some(self.tasks.len() as u64);
    }

    pub fn ingest_pending_approvals(&mut self, value: &serde_json::Value) {
        self.pending_approvals = value
            .as_array()
            .or_else(|| value.get("approvals").and_then(serde_json::Value::as_array))
            .or_else(|| value.get("pending").and_then(serde_json::Value::as_array))
            .map(|items| items.len() as u64);
    }

    pub fn ingest_memory_status(&mut self, value: &serde_json::Value) {
        self.memory_status = value
            .get("status")
            .or_else(|| value.pointer("/memory/status"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
    }

    pub fn ingest_cross_plane_summary(&mut self, value: &serde_json::Value) {
        self.cross_plane_grants_active = value
            .pointer("/grants/active")
            .and_then(serde_json::Value::as_u64);
        self.cross_plane_actions_24h = value
            .pointer("/interop/actions_24h")
            .and_then(serde_json::Value::as_u64);
    }

    pub fn degrade(&mut self, reason: impl Into<String>) {
        self.degraded_reasons.push(reason.into());
    }
}

fn task_summary_from_json(value: &serde_json::Value) -> Option<DaemonTaskSummary> {
    let id = value.get("id").and_then(serde_json::Value::as_str)?;
    let objective = value
        .get("objective")
        .or_else(|| value.get("title"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let status = value
        .get("status")
        .or_else(|| value.get("phase"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let current_phase = value
        .get("current_phase")
        .or_else(|| value.get("currentPhase"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let yolo_mode = value
        .get("yolo_mode")
        .or_else(|| value.get("yoloMode"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let failure_count = value
        .get("failure_count")
        .or_else(|| value.get("failureCount"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    Some(DaemonTaskSummary {
        id: id.to_string(),
        objective,
        status,
        current_phase,
        yolo_mode,
        failure_count,
    })
}

pub async fn refresh_runtime_control_snapshot(
    control_client: &DaemonControlClient,
    projection_client: Option<&DaemonProjectionClient>,
    session_id: Option<&str>,
) -> RuntimeControlSnapshot {
    let mut snapshot = match control_client.status().await {
        Ok(status) => RuntimeControlSnapshot::from_status(&status),
        Err(err) => {
            let mut snapshot = RuntimeControlSnapshot::default();
            snapshot.degrade(format!("daemon control unavailable: {err}"));
            return snapshot;
        }
    };

    match control_client.list_sessions().await {
        Ok(list) => snapshot.ingest_session_ids(list.sessions),
        Err(err) => snapshot.degrade(format!("session list unavailable: {err}")),
    }

    let Some(projection) = projection_client else {
        snapshot.degrade("daemon projection unavailable");
        return snapshot;
    };

    match projection.runtime_control_plane().await {
        Ok(value) => snapshot.ingest_runtime_control_plane(&value),
        Err(err) => snapshot.degrade(format!("runtime projection unavailable: {err}")),
    }
    match projection.task_status().await {
        Ok(value) => snapshot.ingest_task_status(&value),
        Err(err) => snapshot.degrade(format!("task projection unavailable: {err}")),
    }
    match projection.pending_approvals().await {
        Ok(value) => snapshot.ingest_pending_approvals(&value),
        Err(err) => snapshot.degrade(format!("approval projection unavailable: {err}")),
    }
    match projection.memory_status().await {
        Ok(value) => snapshot.ingest_memory_status(&value),
        Err(err) => snapshot.degrade(format!("memory projection unavailable: {err}")),
    }
    match projection.cross_plane_summary().await {
        Ok(value) => snapshot.ingest_cross_plane_summary(&value),
        Err(err) => snapshot.degrade(format!("cross-plane projection unavailable: {err}")),
    }

    if let Some(session_id) = session_id {
        if let Err(err) = projection.current_context(Some(session_id)).await {
            snapshot.degrade(format!("context projection unavailable: {err}"));
        }
    }

    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status() -> DaemonStatus {
        DaemonStatus {
            ok: true,
            protocol_version: 1,
            daemon: "cowd".to_string(),
            active_sessions: 2,
            uptime_secs: 9,
        }
    }

    #[test]
    fn snapshot_extracts_projection_summaries() {
        let mut snapshot = RuntimeControlSnapshot::from_status(&status());
        snapshot.ingest_session_ids(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        snapshot.ingest_runtime_control_plane(&serde_json::json!({
            "diagnostics": {
                "readiness_score": 87,
                "component_count": 12
            }
        }));
        snapshot.ingest_task_status(&serde_json::json!({
            "tasks": [{"id": "t1"}, {"id": "t2"}]
        }));
        snapshot.ingest_pending_approvals(&serde_json::json!([
            {"id": "a1"}
        ]));
        snapshot.ingest_memory_status(&serde_json::json!({
            "status": "available"
        }));
        snapshot.ingest_cross_plane_summary(&serde_json::json!({
            "grants": {"active": 4},
            "interop": {"actions_24h": 7}
        }));

        assert!(snapshot.daemon_running);
        assert_eq!(snapshot.active_sessions, 3);
        assert_eq!(snapshot.runtime_readiness.as_deref(), Some("87%"));
        assert_eq!(snapshot.runtime_components, Some(12));
        assert_eq!(snapshot.task_count, Some(2));
        assert_eq!(snapshot.tasks.len(), 2);
        assert_eq!(snapshot.tasks[0].id, "t1");
        assert_eq!(snapshot.pending_approvals, Some(1));
        assert_eq!(snapshot.memory_status.as_deref(), Some("available"));
        assert_eq!(snapshot.cross_plane_grants_active, Some(4));
        assert_eq!(snapshot.cross_plane_actions_24h, Some(7));
    }

    #[test]
    fn snapshot_tracks_partial_degradation() {
        let mut snapshot = RuntimeControlSnapshot::from_status(&status());
        snapshot.degrade("task projection unavailable");
        snapshot.degrade("memory projection unavailable");

        assert!(snapshot.daemon_running);
        assert_eq!(snapshot.degraded_reasons.len(), 2);
        assert!(snapshot
            .degraded_reasons
            .iter()
            .any(|reason| reason.contains("task")));
    }
}
