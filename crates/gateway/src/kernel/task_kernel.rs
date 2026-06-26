use std::path::PathBuf;

use runtime::{AgentNodeStatus, AgentRunGraph, ReviewVerdict};
use storage::StorageHandle;

#[cfg(test)]
pub(crate) use runtime::task::{TaskPhaseArtifact, TaskPhaseStatus};
pub(crate) use runtime::task::{TaskPhaseRecord, TaskStatus};

pub(crate) type TaskRecord = runtime::task::TaskRecord<AgentRunGraph>;

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
        self.inner.list_as().unwrap_or_default()
    }

    pub(crate) fn current(&self) -> Option<TaskRecord> {
        self.inner.current_as().ok().flatten()
    }

    pub(crate) fn start_goal(
        &self,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        let task = self.inner.start_goal(objective, yolo_mode)?;
        let task = task.decode_graph::<AgentRunGraph>()?;
        let mut graph = AgentRunGraph::from_objective(task.id.clone(), task.objective.clone());
        sync_phase_node(&mut graph, &task, "implementation")?;
        self.inner
            .upsert_agent_graph(&task.id, graph)
            .and_then(runtime::task::TaskRecord::decode_graph)
    }

    pub(crate) fn transition(
        &self,
        task_id: &str,
        status: TaskStatus,
        phase: Option<String>,
        message: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.inner
            .transition(task_id, status, phase, message)
            .and_then(runtime::task::TaskRecord::decode_graph)
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
        let name = name.into();
        let task = self.inner.start_phase(
            task_id,
            name.clone(),
            objective,
            plan,
            acceptance,
            test_commands,
        )?;
        let mut task = task.decode_graph::<AgentRunGraph>()?;
        let task_snapshot = task.clone();
        let task_id = task.id.clone();
        if let Some(graph) = &mut task.agent_graph {
            sync_phase_node(graph, &task_snapshot, &name)?;
            return self.upsert_agent_graph(&task_id, graph.clone());
        }
        Ok(task)
    }

    pub(crate) fn record_phase_artifact(
        &self,
        task_id: &str,
        phase_id: &str,
        kind: impl Into<String>,
        label: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        let kind = kind.into();
        let label = label.into();
        let value = value.into();
        let task = self
            .inner
            .record_phase_artifact(task_id, phase_id, &kind, &label, &value)?;
        let mut task = task.decode_graph::<AgentRunGraph>()?;
        if let Some(graph) = &mut task.agent_graph {
            graph
                .add_evidence(phase_id, kind, label, value)
                .map_err(|error| error.to_string())?;
            return self.upsert_agent_graph(&task.id, graph.clone());
        }
        Ok(task)
    }

    pub(crate) fn review_phase(
        &self,
        task_id: &str,
        phase_id: &str,
        result: impl Into<String>,
        completed: bool,
    ) -> Result<TaskRecord, String> {
        let result = result.into();
        let task = self
            .inner
            .review_phase(task_id, phase_id, result.clone(), completed)?;
        let mut task = task.decode_graph::<AgentRunGraph>()?;
        if let Some(graph) = &mut task.agent_graph {
            if graph.nodes.iter().any(|node| node.id == phase_id) {
                let verdict = if completed {
                    ReviewVerdict::Accept
                } else {
                    ReviewVerdict::Challenge
                };
                graph
                    .add_review(phase_id, "task-reviewer", verdict, result)
                    .map_err(|error| error.to_string())?;
                return self.upsert_agent_graph(&task.id, graph.clone());
            }
        }
        Ok(task)
    }

    pub(crate) fn record_failure(
        &self,
        task_id: &str,
        reason: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        let reason = reason.into();
        let task = self.inner.record_failure(task_id, reason.clone())?;
        let mut task = task.decode_graph::<AgentRunGraph>()?;
        if let Some(graph) = &mut task.agent_graph {
            if let Some(current_phase) = task.current_phase.as_deref() {
                if let Some(node_id) = task
                    .phases
                    .iter()
                    .rev()
                    .find(|phase| phase.name == current_phase)
                    .map(|phase| phase.id.clone())
                {
                    if graph.nodes.iter().any(|node| node.id == node_id) {
                        graph
                            .record_failure(&node_id, reason)
                            .map_err(|error| error.to_string())?;
                        return self.upsert_agent_graph(&task.id, graph.clone());
                    }
                }
            }
        }
        Ok(task)
    }

    pub(crate) fn list_agent_graphs(&self) -> Vec<AgentRunGraph> {
        self.inner.list_agent_graphs_as().unwrap_or_default()
    }

    pub(crate) fn agent_graph(&self, task_id: &str) -> Option<AgentRunGraph> {
        self.inner.agent_graph_as(task_id).ok().flatten()
    }

    pub(crate) fn upsert_agent_graph(
        &self,
        task_id: &str,
        graph: AgentRunGraph,
    ) -> Result<TaskRecord, String> {
        self.inner
            .upsert_agent_graph(task_id, graph)
            .and_then(runtime::task::TaskRecord::decode_graph)
    }
}

fn sync_phase_node(
    graph: &mut AgentRunGraph,
    task: &TaskRecord,
    phase_name: &str,
) -> Result<(), String> {
    let Some(phase) = task.phases.last() else {
        return Ok(());
    };
    graph
        .upsert_phase_node(
            phase.id.clone(),
            phase_name.to_string(),
            phase.objective.clone(),
        )
        .map_err(|error| error.to_string())?;
    if let Some(planner) = graph.nodes.iter_mut().find(|node| node.id == "planner") {
        planner.status = AgentNodeStatus::Completed;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TaskKernel, TaskStatus};

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cowd-task-{label}-{}.db", uuid::Uuid::new_v4()))
    }

    fn legacy_temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cowd-task-{label}-{}.json", uuid::Uuid::new_v4()))
    }

    #[test]
    fn task_kernel_persists_and_restores_started_goal() {
        let path = temp_path("persist");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Ship v0.8.10", true).unwrap();

        let restored = TaskKernel::open(path.clone()).unwrap();
        let current = restored.current().expect("current task should restore");
        assert_eq!(current.id, task.id);
        assert_eq!(current.status, TaskStatus::Running);
        assert!(current.yolo_mode);
        assert!(current.agent_graph.is_some());

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
        let restored = TaskKernel::open(legacy_path.clone()).unwrap();
        assert_eq!(restored.list().len(), 1);

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
    fn task_kernel_can_cancel_and_complete_tasks() {
        let path = temp_path("transition");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Review", false).unwrap();

        let reviewing = kernel
            .transition(
                &task.id,
                TaskStatus::Reviewing,
                Some("review".to_string()),
                "tests passed",
            )
            .unwrap();
        assert_eq!(reviewing.status, TaskStatus::Reviewing);

        let completed = kernel
            .transition(&task.id, TaskStatus::Completed, None, "accepted")
            .unwrap();
        assert_eq!(completed.status, TaskStatus::Completed);
        assert!(kernel.current().is_none());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn task_kernel_records_phase_artifacts_and_review() {
        let path = temp_path("phase");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Ship enterprise workflow", true).unwrap();

        let with_phase = kernel
            .start_phase(
                &task.id,
                "webui-e2e",
                "Cover task workbench browser scenario",
                vec!["Add Playwright fixture".to_string()],
                vec!["E2E passes".to_string()],
                vec!["cargo test -p gateway task_kernel -- --nocapture".to_string()],
            )
            .unwrap();
        let phase = with_phase
            .phases
            .last()
            .expect("phase should exist")
            .clone();
        assert_eq!(phase.name, "webui-e2e");
        assert_eq!(phase.status.as_str(), "running");
        assert_eq!(with_phase.current_phase.as_deref(), Some("webui-e2e"));

        let with_artifact = kernel
            .record_phase_artifact(&task.id, &phase.id, "test", "playwright", "2 passed")
            .unwrap();
        let phase = with_artifact
            .phases
            .iter()
            .find(|candidate| candidate.id == phase.id)
            .unwrap()
            .clone();
        assert_eq!(phase.artifacts[0].label, "playwright");

        let reviewed = kernel
            .review_phase(&task.id, &phase.id, "accepted after gate", true)
            .unwrap();
        let reviewed_phase = reviewed
            .phases
            .iter()
            .find(|candidate| candidate.id == phase.id)
            .unwrap();
        assert_eq!(reviewed_phase.status.as_str(), "completed");
        assert_eq!(
            reviewed_phase.review_result.as_deref(),
            Some("accepted after gate")
        );
        assert!(reviewed
            .audit
            .iter()
            .any(|event| event.event_type == "agent_graph_updated"));
        let graph = reviewed.agent_graph.as_ref().expect("agent graph");
        assert!(graph.nodes.iter().any(|node| node.id == phase.id));
        assert!(graph.evidence.iter().any(|evidence| {
            evidence.node_id == phase.id
                && evidence.reference == "playwright"
                && evidence.summary == "2 passed"
        }));
        assert!(graph.reviews.iter().any(|review| {
            review.node_id == phase.id && review.comment == "accepted after gate"
        }));

        let restored = TaskKernel::open(path.clone()).unwrap();
        let restored_task = restored
            .list()
            .into_iter()
            .find(|t| t.id == task.id)
            .unwrap();
        assert!(restored_task.phases.iter().any(|p| p.id == phase.id));
        assert_eq!(restored.list_agent_graphs().len(), 1);

        let _ = std::fs::remove_file(path);
    }
}
