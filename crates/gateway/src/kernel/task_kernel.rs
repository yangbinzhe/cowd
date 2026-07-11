use std::path::PathBuf;

use harness_contract::execution_graph::ExecutionGraphProjection;
use storage::StorageHandle;

#[cfg(test)]
pub(crate) use runtime::task::{TaskPhaseArtifact, TaskPhaseStatus};
pub(crate) use runtime::task::{TaskPhaseRecord, TaskStatus};

pub(crate) type TaskRecord = runtime::task::TaskRecord;

/// Gateway adapter for task-domain metadata.
///
/// Execution state is owned by Runtime's event-sourced graph host. This kernel
/// may cache a projection returned by that host, but it never creates nodes or
/// advances graph status itself.
#[derive(Debug, Clone)]
pub(crate) struct TaskKernel {
    inner: runtime::task::TaskKernel,
}

impl TaskKernel {
    pub(crate) fn open(path: PathBuf) -> Result<Self, String> {
        Ok(Self {
            inner: runtime::task::TaskKernel::open(path)?,
        })
    }

    pub(crate) fn open_storage_handle(handle: &StorageHandle) -> Result<Self, String> {
        Ok(Self {
            inner: runtime::task::TaskKernel::open_storage_handle(handle)?,
        })
    }

    pub(crate) fn list(&self) -> Vec<TaskRecord> {
        self.inner.list()
    }

    pub(crate) fn current(&self) -> Option<TaskRecord> {
        self.inner.current()
    }

    pub(crate) fn start_goal(
        &self,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        self.inner.start_goal(objective, yolo_mode)
    }

    pub(crate) fn transition(
        &self,
        task_id: &str,
        status: TaskStatus,
        phase: Option<String>,
        message: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.inner.transition(task_id, status, phase, message)
    }

    pub(crate) fn start_phase(
        &self,
        task_id: &str,
        name: impl Into<String>,
        objective: impl Into<String>,
        plan: Vec<String>,
        acceptance: Vec<String>,
        test_commands: Vec<String>,
    ) -> Result<TaskRecord, String> {
        self.inner
            .start_phase(task_id, name, objective, plan, acceptance, test_commands)
    }

    pub(crate) fn record_phase_artifact(
        &self,
        task_id: &str,
        phase_id: &str,
        kind: impl Into<String>,
        label: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.inner
            .record_phase_artifact(task_id, phase_id, kind, label, value)
    }

    pub(crate) fn review_phase(
        &self,
        task_id: &str,
        phase_id: &str,
        result: impl Into<String>,
        completed: bool,
    ) -> Result<TaskRecord, String> {
        self.inner
            .review_phase(task_id, phase_id, result, completed)
    }

    pub(crate) fn record_failure(
        &self,
        task_id: &str,
        reason: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.inner.record_failure(task_id, reason)
    }

    pub(crate) fn execution_graphs(&self) -> Vec<ExecutionGraphProjection> {
        self.inner.execution_graphs()
    }

    pub(crate) fn execution_graph(&self, task_id: &str) -> Option<ExecutionGraphProjection> {
        self.inner.execution_graph(task_id)
    }

    pub(crate) fn record_execution_graph_projection(
        &self,
        task_id: &str,
        projection: ExecutionGraphProjection,
    ) -> Result<TaskRecord, String> {
        self.inner
            .record_execution_graph_projection(task_id, projection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cowd-task-{label}-{}.db", uuid::Uuid::new_v4()))
    }

    fn legacy_temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cowd-task-{label}-{}.json", uuid::Uuid::new_v4()))
    }

    #[test]
    fn task_kernel_persists_domain_state_without_owning_execution() {
        let path = temp_path("persist");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Ship v0.9.473", true).unwrap();

        let restored = TaskKernel::open(path.clone()).unwrap();
        let current = restored.current().expect("current task should restore");
        assert_eq!(current.id, task.id);
        assert_eq!(current.status, TaskStatus::Running);
        assert!(current.yolo_mode);
        assert!(current.execution_graph.is_none());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn task_kernel_maps_legacy_json_path_to_sqlite_db_without_json_write() {
        let legacy_path = legacy_temp_path("legacy-map");
        let db_path = legacy_path.with_extension("db");
        let kernel = TaskKernel::open(legacy_path.clone()).unwrap();
        kernel.start_goal("Use sqlite task store", true).unwrap();

        assert!(db_path.is_file());
        assert!(!legacy_path.exists());
        assert_eq!(TaskKernel::open(legacy_path).unwrap().list().len(), 1);
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn task_kernel_blocks_after_three_failures() {
        let path = temp_path("blocked");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Recover failing task", true).unwrap();

        kernel.record_failure(&task.id, "first").unwrap();
        kernel.record_failure(&task.id, "second").unwrap();
        let blocked = kernel
            .record_failure(&task.id, "external input required")
            .unwrap();

        assert_eq!(blocked.status, TaskStatus::Blocked);
        assert_eq!(
            blocked.blocker_reason.as_deref(),
            Some("external input required")
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn task_phase_metadata_does_not_mutate_execution_projection() {
        let path = temp_path("phase");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Ship enterprise workflow", true).unwrap();
        let phase_task = kernel
            .start_phase(
                &task.id,
                "webui-e2e",
                "Cover browser scenario",
                vec!["Add fixture".to_string()],
                vec!["E2E passes".to_string()],
                vec!["cargo test -p gateway task_kernel".to_string()],
            )
            .unwrap();
        let phase = phase_task.phases.last().unwrap();
        kernel
            .record_phase_artifact(&task.id, &phase.id, "test", "playwright", "2 passed")
            .unwrap();
        let reviewed = kernel
            .review_phase(&task.id, &phase.id, "accepted", true)
            .unwrap();

        assert!(reviewed.execution_graph.is_none());
        assert_eq!(
            reviewed.phases.last().unwrap().status,
            TaskPhaseStatus::Completed
        );
        let _ = std::fs::remove_file(path);
    }
}
