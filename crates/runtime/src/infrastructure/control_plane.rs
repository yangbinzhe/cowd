use std::sync::OnceLock;

use crate::{
    cowd_dirs, CronEntry, CronRegistry, Task, TaskPacket, TaskPacketValidationError, TaskRegistry,
    Team, TeamRegistry, Worker, WorkerReadySnapshot, WorkerRegistry, WorkerTaskReceipt,
};

#[derive(Debug)]
pub struct RuntimeControlPlane {
    tasks: TaskRegistry,
    workers: WorkerRegistry,
    teams: TeamRegistry,
    crons: CronRegistry,
}

impl Default for RuntimeControlPlane {
    fn default() -> Self {
        Self {
            tasks: TaskRegistry::new(),
            workers: WorkerRegistry::new(),
            teams: TeamRegistry::new(),
            crons: CronRegistry::new(),
        }
    }
}

impl RuntimeControlPlane {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tasks(&self) -> &TaskRegistry {
        &self.tasks
    }

    pub fn create_task(&self, prompt: &str, description: Option<&str>) -> Task {
        self.tasks.create(prompt, description)
    }

    pub fn create_task_from_packet(
        &self,
        packet: TaskPacket,
    ) -> Result<Task, TaskPacketValidationError> {
        self.tasks.create_from_packet(packet)
    }

    pub fn create_worker(
        &self,
        cwd: &str,
        trusted_roots: &[String],
        auto_recover_prompt_misdelivery: bool,
    ) -> Worker {
        let worker = self
            .workers
            .create(cwd, trusted_roots, auto_recover_prompt_misdelivery);
        let _ = persist_worker_state(&worker);
        worker
    }

    pub fn get_worker(&self, worker_id: &str) -> Option<Worker> {
        self.workers.get(worker_id)
    }

    pub fn observe_worker(&self, worker_id: &str, screen_text: &str) -> Result<Worker, String> {
        let worker = self.workers.observe(worker_id, screen_text)?;
        let _ = persist_worker_state(&worker);
        Ok(worker)
    }

    pub fn resolve_worker_trust(&self, worker_id: &str) -> Result<Worker, String> {
        let worker = self.workers.resolve_trust(worker_id)?;
        let _ = persist_worker_state(&worker);
        Ok(worker)
    }

    pub fn await_worker_ready(&self, worker_id: &str) -> Result<WorkerReadySnapshot, String> {
        self.workers.await_ready(worker_id)
    }

    pub fn send_worker_prompt(
        &self,
        worker_id: &str,
        prompt: Option<&str>,
        task_receipt: Option<WorkerTaskReceipt>,
    ) -> Result<Worker, String> {
        let worker = self.workers.send_prompt(worker_id, prompt, task_receipt)?;
        let _ = persist_worker_state(&worker);
        Ok(worker)
    }

    pub fn restart_worker(&self, worker_id: &str) -> Result<Worker, String> {
        let worker = self.workers.restart(worker_id)?;
        let _ = persist_worker_state(&worker);
        Ok(worker)
    }

    pub fn terminate_worker(&self, worker_id: &str) -> Result<Worker, String> {
        let worker = self.workers.terminate(worker_id)?;
        let _ = persist_worker_state(&worker);
        Ok(worker)
    }

    pub fn observe_worker_completion(
        &self,
        worker_id: &str,
        finish_reason: &str,
        tokens_output: u64,
    ) -> Result<Worker, String> {
        self.workers
            .observe_completion(worker_id, finish_reason, tokens_output)
    }

    pub fn create_team(&self, name: &str, task_ids: Vec<String>) -> Team {
        let team = self.teams.create(name, task_ids);
        for task_id in &team.task_ids {
            let _ = self.tasks.assign_team(task_id, &team.team_id);
        }
        team
    }

    pub fn delete_team(&self, team_id: &str) -> Result<Team, String> {
        self.teams.delete(team_id)
    }

    pub fn create_cron(
        &self,
        schedule: &str,
        prompt: &str,
        description: Option<&str>,
    ) -> CronEntry {
        self.crons.create(schedule, prompt, description)
    }

    pub fn delete_cron(&self, cron_id: &str) -> Result<CronEntry, String> {
        self.crons.delete(cron_id)
    }

    pub fn list_crons(&self, enabled_only: bool) -> Vec<CronEntry> {
        self.crons.list(enabled_only)
    }
}

pub fn global_runtime_control_plane() -> &'static RuntimeControlPlane {
    static CONTROL_PLANE: OnceLock<RuntimeControlPlane> = OnceLock::new();
    CONTROL_PLANE.get_or_init(RuntimeControlPlane::new)
}

pub fn global_task_registry() -> &'static TaskRegistry {
    global_runtime_control_plane().tasks()
}

fn persist_worker_state(worker: &Worker) -> std::io::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let state_path = cowd_dirs::worker_state_path();
    if let Some(parent) = state_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let state = serde_json::json!({
        "worker_id": worker.worker_id,
        "status": worker.status.to_string(),
        "is_ready": matches!(worker.status, crate::WorkerStatus::ReadyForPrompt),
        "trust_gate_cleared": worker.trust_gate_cleared,
        "seconds_since_update": now.saturating_sub(worker.updated_at),
    });
    std::fs::write(&state_path, serde_json::to_string_pretty(&state)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_plane_owns_task_team_and_cron_state() {
        let plane = RuntimeControlPlane::new();
        let task = plane.create_task("implement runtime control plane", Some("test"));
        let team = plane.create_team("runtime-team", vec![task.task_id.clone()]);
        assert_eq!(
            plane.tasks().get(&task.task_id).unwrap().team_id.as_deref(),
            Some(team.team_id.as_str())
        );

        let cron = plane.create_cron("*/5 * * * *", "check state", Some("test cron"));
        assert_eq!(plane.list_crons(false).len(), 1);
        assert_eq!(
            plane.delete_cron(&cron.cron_id).unwrap().cron_id,
            cron.cron_id
        );
    }
}
