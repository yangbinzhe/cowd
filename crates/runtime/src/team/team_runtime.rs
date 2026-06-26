//! Runtime-owned team lifecycle service.
//!
//! TeamRuntime consumes a `CollaborationDecision` and turns it into a concrete
//! runtime team projection. It owns team status, role agents, events, and human
//! control commands; execution backends can attach to this service without
//! redefining team state.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::{
    cowd_dirs, global_agent_event_bus, global_agent_lifecycle_service, global_agent_task_mailbox,
    global_mission_evidence_bus, global_runtime_control_plane, AgentLifecycleEvent,
    AgentProgressEvent, AgentSnapshot, AgentTask, CollaborationDecision, CollaborationPlan,
    CollaborationTemplateId, MissionEvidenceRef,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamRuntimeStatus {
    Planned,
    Running,
    Paused,
    ReviewRequested,
    Completed,
    Cancelled,
    Failed,
}

impl TeamRuntimeStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }

    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::ReviewRequested => "review_requested",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRuntimeAgent {
    pub role_id: String,
    pub responsibility: String,
    pub allowed_tools: Vec<String>,
    pub evidence_duties: Vec<String>,
    pub status: TeamRuntimeStatus,
    pub agent_id: Option<String>,
    pub latest_summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamRuntimeSynthesisStatus {
    NotStarted,
    Deterministic,
    ModelAssistedPending,
    ModelAssistedCompleted,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRuntimeRoleSummary {
    pub role_id: String,
    pub agent_id: Option<String>,
    pub status: TeamRuntimeStatus,
    pub summary: Option<String>,
    pub output_file: Option<String>,
    pub evidence_refs: Vec<String>,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRuntimeExecutionSummary {
    pub team_id: String,
    pub session_id: String,
    pub objective: String,
    pub status: TeamRuntimeStatus,
    pub role_summaries: Vec<TeamRuntimeRoleSummary>,
    pub completed_agents: Vec<String>,
    pub failed_agents: Vec<String>,
    pub cancelled_agents: Vec<String>,
    pub output_files: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub blocker_summary: Option<String>,
    pub failed_reasons: Vec<String>,
    pub synthesis_status: TeamRuntimeSynthesisStatus,
    pub synthesis_output_file: Option<String>,
    pub review_required: bool,
    pub review_reason: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRuntimeEvent {
    pub team_id: String,
    pub event_type: String,
    pub message: String,
    pub sequence: u64,
    pub emitted_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRuntimeSnapshot {
    pub team_id: String,
    pub session_id: String,
    pub task_id: String,
    pub objective: String,
    pub template_id: CollaborationTemplateId,
    pub status: TeamRuntimeStatus,
    pub plan: CollaborationPlan,
    pub agents: Vec<TeamRuntimeAgent>,
    pub pending_inputs: Vec<String>,
    pub review_notes: Vec<String>,
    pub merge_summary: Option<String>,
    pub execution_summary: Option<TeamRuntimeExecutionSummary>,
    pub result_artifact_file: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartTeamRuntimeRequest {
    pub session_id: String,
    pub objective: String,
    pub collaboration_decision: CollaborationDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartTeamRuntimeAgentRequest {
    pub team_id: String,
    pub session_id: String,
    pub objective: String,
    pub role_id: String,
    pub responsibility: String,
    pub allowed_tools: Vec<String>,
    pub evidence_duties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRuntimeCommandReceipt {
    pub team_id: String,
    pub command: String,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationAgentRunProjection {
    pub role_id: String,
    pub agent_id: Option<String>,
    pub status: TeamRuntimeStatus,
    pub latest_summary: Option<String>,
    pub output_file: Option<String>,
    pub evidence_refs: Vec<String>,
    pub lifecycle_events: Vec<AgentLifecycleEvent>,
    pub progress_events: Vec<AgentProgressEvent>,
    pub tasks: Vec<AgentTask>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationRunProjection {
    pub kind: String,
    pub team: TeamRuntimeSnapshot,
    pub team_events: Vec<TeamRuntimeEvent>,
    pub agent_runs: Vec<CollaborationAgentRunProjection>,
    pub mission_evidence: Vec<MissionEvidenceRef>,
    pub execution_summary: Option<TeamRuntimeExecutionSummary>,
    pub synthesis_ready: bool,
    pub control_actions: Vec<String>,
}

#[derive(Debug, Clone)]
struct TeamRuntimeRecord {
    snapshot: TeamRuntimeSnapshot,
    events: Vec<TeamRuntimeEvent>,
    next_sequence: u64,
}

#[derive(Debug, Default)]
pub struct TeamRuntimeService {
    runs: Mutex<BTreeMap<String, TeamRuntimeRecord>>,
}

impl TeamRuntimeService {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(&self, request: StartTeamRuntimeRequest) -> Result<TeamRuntimeSnapshot, String> {
        if request.session_id.trim().is_empty() {
            return Err("session_id must not be empty".to_string());
        }
        if request.objective.trim().is_empty() {
            return Err("objective must not be empty".to_string());
        }
        let plane = global_runtime_control_plane();
        let task = plane.create_task(
            &request.objective,
            Some(request.collaboration_decision.rationale.as_str()),
        );
        let team = plane.create_team(
            request.collaboration_decision.template_id.as_str(),
            vec![task.task_id.clone()],
        );
        let now = now_ms();
        let agents = request
            .collaboration_decision
            .plan
            .agents
            .iter()
            .map(|agent| TeamRuntimeAgent {
                role_id: agent.role_id.clone(),
                responsibility: agent.responsibility.clone(),
                allowed_tools: agent.allowed_tools.clone(),
                evidence_duties: agent.evidence_duties.clone(),
                status: TeamRuntimeStatus::Planned,
                agent_id: None,
                latest_summary: None,
            })
            .collect::<Vec<_>>();
        let mut snapshot = TeamRuntimeSnapshot {
            team_id: team.team_id,
            session_id: request.session_id,
            task_id: task.task_id,
            objective: request.objective,
            template_id: request.collaboration_decision.template_id,
            status: TeamRuntimeStatus::Running,
            plan: request.collaboration_decision.plan,
            agents,
            pending_inputs: Vec::new(),
            review_notes: Vec::new(),
            merge_summary: None,
            execution_summary: None,
            result_artifact_file: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        for agent in &mut snapshot.agents {
            agent.status = TeamRuntimeStatus::Running;
        }
        let team_id = snapshot.team_id.clone();
        let mut record = TeamRuntimeRecord {
            snapshot: snapshot.clone(),
            events: Vec::new(),
            next_sequence: 0,
        };
        record.push_event("team.started", "team runtime started");
        self.runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(team_id, record);
        Ok(snapshot)
    }

    pub fn start_with_agent_spawner<F>(
        &self,
        request: StartTeamRuntimeRequest,
        mut spawner: F,
    ) -> Result<TeamRuntimeSnapshot, String>
    where
        F: FnMut(StartTeamRuntimeAgentRequest) -> Result<AgentSnapshot, String>,
    {
        let snapshot = self.start(request)?;
        let mut bindings: Vec<(String, String)> = Vec::new();
        for agent in &snapshot.agents {
            let spawn_request = StartTeamRuntimeAgentRequest {
                team_id: snapshot.team_id.clone(),
                session_id: snapshot.session_id.clone(),
                objective: snapshot.objective.clone(),
                role_id: agent.role_id.clone(),
                responsibility: agent.responsibility.clone(),
                allowed_tools: agent.allowed_tools.clone(),
                evidence_duties: agent.evidence_duties.clone(),
            };
            let spawned = match spawner(spawn_request) {
                Ok(spawned) => spawned,
                Err(error) => {
                    for (_, agent_id) in &bindings {
                        let _ = global_agent_lifecycle_service().cancel(agent_id);
                    }
                    let _ = self.with_record(&snapshot.team_id, "bind_agents_failed", |record| {
                        record.snapshot.status = TeamRuntimeStatus::Failed;
                        for (role_id, agent_id) in bindings {
                            if let Some(agent) = record
                                .snapshot
                                .agents
                                .iter_mut()
                                .find(|agent| agent.role_id == role_id)
                            {
                                agent.agent_id = Some(agent_id);
                                agent.status = TeamRuntimeStatus::Cancelled;
                            }
                        }
                        for agent in &mut record.snapshot.agents {
                            if !agent.status.is_terminal() {
                                agent.status = TeamRuntimeStatus::Failed;
                            }
                        }
                        record.touch();
                        record.push_event(
                            "team.agents_bind_failed",
                            format!("team runtime agent binding failed: {error}"),
                        );
                        "agent binding failed".to_string()
                    });
                    return Err(error);
                }
            };
            bindings.push((agent.role_id.clone(), spawned.agent_id));
        }
        self.with_record(&snapshot.team_id, "bind_agents", |record| {
            for (role_id, agent_id) in bindings {
                if let Some(agent) = record
                    .snapshot
                    .agents
                    .iter_mut()
                    .find(|agent| agent.role_id == role_id)
                {
                    agent.agent_id = Some(agent_id);
                    agent.status = TeamRuntimeStatus::Running;
                }
            }
            record.touch();
            record.push_event(
                "team.agents_bound",
                "team runtime agents bound to lifecycle",
            );
            "agents bound".to_string()
        })?;
        self.get(&snapshot.team_id)
            .ok_or_else(|| format!("team runtime not found: {}", snapshot.team_id))
    }

    #[must_use]
    pub fn get(&self, team_id: &str) -> Option<TeamRuntimeSnapshot> {
        let _ = self.refresh_from_agent_lifecycle(team_id);
        self.runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(team_id)
            .map(|record| record.snapshot.clone())
    }

    #[must_use]
    pub fn list(&self) -> Vec<TeamRuntimeSnapshot> {
        let team_ids = self
            .runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for team_id in team_ids {
            let _ = self.refresh_from_agent_lifecycle(&team_id);
        }
        self.runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|record| record.snapshot.clone())
            .collect()
    }

    #[must_use]
    pub fn events(&self, team_id: &str) -> Option<Vec<TeamRuntimeEvent>> {
        self.runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(team_id)
            .map(|record| record.events.clone())
    }

    pub fn collaboration_run(&self, team_id: &str) -> Result<CollaborationRunProjection, String> {
        let team = self
            .get(team_id)
            .ok_or_else(|| format!("team runtime not found: {team_id}"))?;
        let team_events = self.events(team_id).unwrap_or_default();
        let mission_evidence = global_mission_evidence_bus().list_for_team(team_id);
        let team_tasks = global_agent_task_mailbox().list_for_team(team_id);
        let team_progress_events = global_agent_event_bus().list_for_team(team_id);
        let agent_runs = team
            .agents
            .iter()
            .map(|agent| {
                let lifecycle_events = agent
                    .agent_id
                    .as_deref()
                    .and_then(|agent_id| global_agent_lifecycle_service().events(agent_id))
                    .unwrap_or_default();
                let progress_events = agent
                    .agent_id
                    .as_deref()
                    .map(|agent_id| global_agent_event_bus().list_for_agent(agent_id))
                    .unwrap_or_else(|| {
                        team_progress_events
                            .iter()
                            .filter(|event| event.role_id == agent.role_id)
                            .cloned()
                            .collect()
                    });
                let tasks = team_tasks
                    .iter()
                    .filter(|task| {
                        task.role_id == agent.role_id
                            || task.agent_id.as_deref() == agent.agent_id.as_deref()
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                CollaborationAgentRunProjection {
                    role_id: agent.role_id.clone(),
                    agent_id: agent.agent_id.clone(),
                    status: agent.status.clone(),
                    latest_summary: agent.latest_summary.clone(),
                    output_file: agent
                        .agent_id
                        .as_deref()
                        .and_then(|agent_id| global_agent_lifecycle_service().get(agent_id))
                        .map(|snapshot| snapshot.output_file),
                    evidence_refs: agent.evidence_duties.clone(),
                    lifecycle_events,
                    progress_events,
                    tasks,
                }
            })
            .collect::<Vec<_>>();
        let synthesis_ready = !agent_runs.is_empty()
            && agent_runs.iter().all(|agent| {
                agent.status.is_terminal() || agent.status == TeamRuntimeStatus::Running
            })
            && (!team_tasks.is_empty() || !mission_evidence.is_empty());
        let mut control_actions = vec![
            "inspect".to_string(),
            "synthesis".to_string(),
            "handoff".to_string(),
            "cancel".to_string(),
        ];
        if team.status == TeamRuntimeStatus::Paused {
            control_actions.push("resume".to_string());
        } else if !team.status.is_terminal() {
            control_actions.push("pause".to_string());
        }
        Ok(CollaborationRunProjection {
            kind: "runtime.collaboration_run".to_string(),
            execution_summary: team.execution_summary.clone(),
            team,
            team_events,
            agent_runs,
            mission_evidence,
            synthesis_ready,
            control_actions,
        })
    }

    #[must_use]
    pub fn collaboration_projection(&self) -> serde_json::Value {
        let runs = self
            .list()
            .into_iter()
            .filter_map(|team| self.collaboration_run(&team.team_id).ok())
            .collect::<Vec<_>>();
        serde_json::json!({
            "kind": "runtime.collaboration_runs",
            "count": runs.len(),
            "runs": runs,
        })
    }

    pub fn append_input(
        &self,
        team_id: &str,
        input: impl Into<String>,
    ) -> Result<TeamRuntimeCommandReceipt, String> {
        let input = input.into();
        if input.trim().is_empty() {
            return Err("input must not be empty".to_string());
        }
        self.with_record(team_id, "append_input", |record| {
            record.snapshot.pending_inputs.push(input);
            record.touch();
            record.push_event("team.input_appended", "input appended to team runtime");
            "input accepted".to_string()
        })
    }

    pub fn pause(&self, team_id: &str) -> Result<TeamRuntimeCommandReceipt, String> {
        self.transition(team_id, "pause", TeamRuntimeStatus::Paused, "team paused")
    }

    pub fn resume(&self, team_id: &str) -> Result<TeamRuntimeCommandReceipt, String> {
        self.transition(
            team_id,
            "resume",
            TeamRuntimeStatus::Running,
            "team resumed",
        )
    }

    pub fn cancel(&self, team_id: &str) -> Result<TeamRuntimeCommandReceipt, String> {
        self.with_record(team_id, "cancel", |record| {
            let agent_ids = record
                .snapshot
                .agents
                .iter()
                .filter_map(|agent| agent.agent_id.clone())
                .collect::<Vec<_>>();
            for agent_id in agent_ids {
                let _ = global_agent_lifecycle_service().cancel(&agent_id);
            }
            record.snapshot.status = TeamRuntimeStatus::Cancelled;
            for agent in &mut record.snapshot.agents {
                if !agent.status.is_terminal() {
                    agent.status = TeamRuntimeStatus::Cancelled;
                }
            }
            record.touch();
            record.push_event("team.cancelled", "team runtime cancelled");
            "team cancelled".to_string()
        })
    }

    pub fn request_review(
        &self,
        team_id: &str,
        note: impl Into<String>,
    ) -> Result<TeamRuntimeCommandReceipt, String> {
        let note = note.into();
        self.with_record(team_id, "request_review", |record| {
            record.snapshot.status = TeamRuntimeStatus::ReviewRequested;
            record.snapshot.review_notes.push(note);
            record.touch();
            record.push_event("team.review_requested", "team review requested");
            "review requested".to_string()
        })
    }

    pub fn handoff(
        &self,
        team_id: &str,
        target: Option<String>,
        note: Option<String>,
    ) -> Result<TeamRuntimeCommandReceipt, String> {
        let target = target
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "human-agent".to_string());
        let note = note
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "handoff requested".to_string());
        self.with_record(team_id, "handoff", |record| {
            record
                .snapshot
                .review_notes
                .push(format!("{target}: {note}"));
            record.touch();
            record.push_event(
                "team.handoff_requested",
                format!("handoff requested for {target}: {note}"),
            );
            format!("handoff requested for {target}")
        })
    }

    pub fn complete_merge(
        &self,
        team_id: &str,
        summary: impl Into<String>,
    ) -> Result<TeamRuntimeCommandReceipt, String> {
        let summary = summary.into();
        if summary.trim().is_empty() {
            return Err("summary must not be empty".to_string());
        }
        self.with_record(team_id, "complete_merge", |record| {
            record.snapshot.status = TeamRuntimeStatus::Completed;
            record.snapshot.merge_summary = Some(summary);
            for agent in &mut record.snapshot.agents {
                if !agent.status.is_terminal() {
                    agent.status = TeamRuntimeStatus::Completed;
                }
            }
            let execution_summary = build_execution_summary(&record.snapshot);
            match write_execution_summary_artifact(&execution_summary) {
                Ok(path) => {
                    record.snapshot.result_artifact_file = Some(path.display().to_string());
                    record.snapshot.execution_summary = Some(TeamRuntimeExecutionSummary {
                        synthesis_output_file: Some(path.display().to_string()),
                        ..execution_summary
                    });
                }
                Err(error) => {
                    record.snapshot.execution_summary = Some(TeamRuntimeExecutionSummary {
                        synthesis_status: TeamRuntimeSynthesisStatus::Failed,
                        review_required: true,
                        review_reason: Some(format!("team summary artifact write failed: {error}")),
                        ..execution_summary
                    });
                }
            }
            record.touch();
            record.push_event("team.completed", "team merge completed");
            "merge completed".to_string()
        })
    }

    pub fn finalize_execution_summary(
        &self,
        team_id: &str,
    ) -> Result<TeamRuntimeExecutionSummary, String> {
        let _ = self.refresh_from_agent_lifecycle(team_id);
        let mut runs = self
            .runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = runs
            .get_mut(team_id)
            .ok_or_else(|| format!("team runtime not found: {team_id}"))?;
        let mut summary = build_execution_summary(&record.snapshot);
        let artifact = write_execution_summary_artifact(&summary)?;
        summary.synthesis_output_file = Some(artifact.display().to_string());
        record.snapshot.result_artifact_file = Some(artifact.display().to_string());
        record.snapshot.execution_summary = Some(summary.clone());
        record.push_event(
            "team.summary.finalized",
            "team deterministic execution summary finalized",
        );
        record.touch();
        Ok(summary)
    }

    pub fn refresh_from_agent_lifecycle(
        &self,
        team_id: &str,
    ) -> Result<TeamRuntimeSnapshot, String> {
        let mut runs = self
            .runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = runs
            .get_mut(team_id)
            .ok_or_else(|| format!("team runtime not found: {team_id}"))?;
        if record.snapshot.status.is_terminal() {
            return Ok(record.snapshot.clone());
        }

        let mut changed = false;
        for agent in &mut record.snapshot.agents {
            let Some(agent_id) = agent.agent_id.as_deref() else {
                continue;
            };
            let Some(snapshot) = global_agent_lifecycle_service().get(agent_id) else {
                continue;
            };
            let next_status = team_status_from_agent_status(&snapshot.status);
            if agent.status != next_status {
                agent.status = next_status;
                changed = true;
            }
            let next_summary = agent_summary_from_snapshot(&snapshot);
            if agent.latest_summary != next_summary {
                agent.latest_summary = next_summary;
                changed = true;
            }
        }

        let all_terminal = !record.snapshot.agents.is_empty()
            && record
                .snapshot
                .agents
                .iter()
                .all(|agent| agent.status.is_terminal());
        if all_terminal {
            let has_failed = record
                .snapshot
                .agents
                .iter()
                .any(|agent| agent.status == TeamRuntimeStatus::Failed);
            let has_cancelled = record
                .snapshot
                .agents
                .iter()
                .any(|agent| agent.status == TeamRuntimeStatus::Cancelled);
            record.snapshot.status = if has_failed {
                TeamRuntimeStatus::Failed
            } else if has_cancelled {
                TeamRuntimeStatus::Cancelled
            } else {
                TeamRuntimeStatus::Completed
            };
            let mut summary = build_execution_summary(&record.snapshot);
            match write_execution_summary_artifact(&summary) {
                Ok(path) => {
                    summary.synthesis_output_file = Some(path.display().to_string());
                    record.snapshot.result_artifact_file = Some(path.display().to_string());
                    record.snapshot.execution_summary = Some(summary);
                }
                Err(error) => {
                    summary.synthesis_status = TeamRuntimeSynthesisStatus::Failed;
                    summary.review_required = true;
                    summary.review_reason =
                        Some(format!("team summary artifact write failed: {error}"));
                    record.snapshot.execution_summary = Some(summary);
                }
            }
            record.push_event(
                "team.summary.auto_finalized",
                "team runtime auto-finalized from agent lifecycle",
            );
            changed = true;
        }

        if changed {
            record.touch();
        }
        Ok(record.snapshot.clone())
    }

    pub fn projection(&self) -> serde_json::Value {
        let teams = self.list();
        serde_json::json!({
            "kind": "runtime.teams",
            "count": teams.len(),
            "teams": teams,
            "collaboration_runs": self.collaboration_projection(),
        })
    }

    fn transition(
        &self,
        team_id: &str,
        command: &str,
        status: TeamRuntimeStatus,
        message: &str,
    ) -> Result<TeamRuntimeCommandReceipt, String> {
        self.with_record(team_id, command, |record| {
            record.snapshot.status = status.clone();
            record.touch();
            record.push_event(format!("team.{}", status.as_str()), message);
            message.to_string()
        })
    }

    fn with_record(
        &self,
        team_id: &str,
        command: &str,
        update: impl FnOnce(&mut TeamRuntimeRecord) -> String,
    ) -> Result<TeamRuntimeCommandReceipt, String> {
        let mut runs = self
            .runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = runs
            .get_mut(team_id)
            .ok_or_else(|| format!("team runtime not found: {team_id}"))?;
        if record.snapshot.status.is_terminal() && command != "complete_merge" {
            return Ok(TeamRuntimeCommandReceipt {
                team_id: team_id.to_string(),
                command: command.to_string(),
                status: "noop".to_string(),
                message: format!("team is already {}", record.snapshot.status.as_str()),
            });
        }
        let message = update(record);
        Ok(TeamRuntimeCommandReceipt {
            team_id: team_id.to_string(),
            command: command.to_string(),
            status: "accepted".to_string(),
            message,
        })
    }
}

impl TeamRuntimeRecord {
    fn touch(&mut self) {
        self.snapshot.updated_at_ms = now_ms();
    }

    fn push_event(&mut self, event_type: impl Into<String>, message: impl Into<String>) {
        let event = TeamRuntimeEvent {
            team_id: self.snapshot.team_id.clone(),
            event_type: event_type.into(),
            message: message.into(),
            sequence: self.next_sequence,
            emitted_at_ms: now_ms(),
        };
        self.next_sequence += 1;
        self.events.push(event);
    }
}

pub fn global_team_runtime_service() -> &'static TeamRuntimeService {
    static SERVICE: OnceLock<TeamRuntimeService> = OnceLock::new();
    SERVICE.get_or_init(TeamRuntimeService::new)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn build_execution_summary(snapshot: &TeamRuntimeSnapshot) -> TeamRuntimeExecutionSummary {
    let role_summaries = snapshot
        .agents
        .iter()
        .map(|agent| TeamRuntimeRoleSummary {
            role_id: agent.role_id.clone(),
            agent_id: agent.agent_id.clone(),
            status: agent.status.clone(),
            summary: agent.latest_summary.clone(),
            output_file: agent
                .agent_id
                .as_deref()
                .and_then(|agent_id| global_agent_lifecycle_service().get(agent_id))
                .map(|snapshot| snapshot.output_file),
            evidence_refs: agent
                .evidence_duties
                .iter()
                .map(|duty| {
                    format!(
                        "team:{}:role:{}:evidence:{duty}",
                        snapshot.team_id, agent.role_id
                    )
                })
                .collect(),
            blocker: if agent.status == TeamRuntimeStatus::Failed {
                Some(format!(
                    "role {} failed or did not produce terminal output",
                    agent.role_id
                ))
            } else {
                None
            },
        })
        .collect::<Vec<_>>();
    let output_files = role_summaries
        .iter()
        .filter_map(|role| role.output_file.clone())
        .collect::<Vec<_>>();
    let evidence_refs = role_summaries
        .iter()
        .flat_map(|role| role.evidence_refs.clone())
        .collect::<Vec<_>>();
    let failed_reasons = role_summaries
        .iter()
        .filter_map(|role| role.blocker.clone())
        .collect::<Vec<_>>();
    let review_required = snapshot.status == TeamRuntimeStatus::ReviewRequested
        || !failed_reasons.is_empty()
        || snapshot
            .agents
            .iter()
            .any(|agent| !agent.status.is_terminal() && agent.status != TeamRuntimeStatus::Running);
    TeamRuntimeExecutionSummary {
        team_id: snapshot.team_id.clone(),
        session_id: snapshot.session_id.clone(),
        objective: snapshot.objective.clone(),
        status: snapshot.status.clone(),
        role_summaries,
        completed_agents: snapshot
            .agents
            .iter()
            .filter(|agent| agent.status == TeamRuntimeStatus::Completed)
            .filter_map(|agent| agent.agent_id.clone())
            .collect(),
        failed_agents: snapshot
            .agents
            .iter()
            .filter(|agent| agent.status == TeamRuntimeStatus::Failed)
            .filter_map(|agent| agent.agent_id.clone())
            .collect(),
        cancelled_agents: snapshot
            .agents
            .iter()
            .filter(|agent| agent.status == TeamRuntimeStatus::Cancelled)
            .filter_map(|agent| agent.agent_id.clone())
            .collect(),
        output_files,
        evidence_refs,
        blocker_summary: if failed_reasons.is_empty() {
            None
        } else {
            Some(failed_reasons.join("; "))
        },
        failed_reasons,
        synthesis_status: TeamRuntimeSynthesisStatus::Deterministic,
        synthesis_output_file: None,
        review_required,
        review_reason: if review_required {
            Some("deterministic summary requires human or model-assisted review".to_string())
        } else {
            None
        },
        created_at_ms: now_ms(),
    }
}

fn team_status_from_agent_status(status: &str) -> TeamRuntimeStatus {
    match status {
        "completed" | "finished" => TeamRuntimeStatus::Completed,
        "failed" => TeamRuntimeStatus::Failed,
        "cancelled" | "canceled" => TeamRuntimeStatus::Cancelled,
        "queued" | "planned" => TeamRuntimeStatus::Planned,
        _ => TeamRuntimeStatus::Running,
    }
}

fn agent_summary_from_snapshot(snapshot: &AgentSnapshot) -> Option<String> {
    if let Some(error) = snapshot
        .error
        .as_ref()
        .filter(|error| !error.trim().is_empty())
    {
        return Some(error.clone());
    }
    if let Some(blocker) = snapshot.current_blocker.as_ref() {
        return Some(format!("{blocker:?}"));
    }
    if !snapshot.derived_state.trim().is_empty() {
        return Some(snapshot.derived_state.clone());
    }
    None
}

fn write_execution_summary_artifact(
    summary: &TeamRuntimeExecutionSummary,
) -> Result<PathBuf, String> {
    let dir = cowd_dirs::user_agents_dir().join("team-results");
    std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
    let path = dir.join(format!("{}-summary.json", summary.team_id));
    let payload = serde_json::to_string_pretty(summary).map_err(|error| error.to_string())?;
    std::fs::write(&path, payload).map_err(|error| error.to_string())?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CollaborationTemplateMatcher;
    use harness_contract::strategy::{decide_strategy, StrategyInput};

    #[test]
    fn team_runtime_starts_from_collaboration_template_and_tracks_commands() {
        let service = TeamRuntimeService::new();
        let prompt = "implement runtime refactor then compile and test";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);

        let snapshot = service
            .start(StartTeamRuntimeRequest {
                session_id: "session-team-runtime".to_string(),
                objective: prompt.to_string(),
                collaboration_decision: decision,
            })
            .expect("team runtime starts");

        assert_eq!(snapshot.status, TeamRuntimeStatus::Running);
        assert_eq!(
            snapshot.template_id,
            CollaborationTemplateId::ImplementationReviewFix
        );
        assert!(snapshot
            .agents
            .iter()
            .any(|agent| agent.role_id == "reviewer"));
        assert!(global_runtime_control_plane()
            .tasks()
            .get(&snapshot.task_id)
            .unwrap()
            .team_id
            .as_deref()
            .is_some());

        service
            .append_input(&snapshot.team_id, "focus reviewer on boundary regressions")
            .expect("input appended");
        service
            .request_review(&snapshot.team_id, "review before merge")
            .expect("review requested");
        let reviewed = service.get(&snapshot.team_id).expect("team snapshot");
        assert_eq!(reviewed.status, TeamRuntimeStatus::ReviewRequested);
        assert_eq!(reviewed.pending_inputs.len(), 1);
        assert_eq!(reviewed.review_notes.len(), 1);

        service
            .complete_merge(&snapshot.team_id, "implemented and reviewed")
            .expect("merge completed");
        let completed = service.get(&snapshot.team_id).expect("completed snapshot");
        assert_eq!(completed.status, TeamRuntimeStatus::Completed);
        assert!(completed
            .agents
            .iter()
            .all(|agent| agent.status == TeamRuntimeStatus::Completed));
        assert!(service.events(&snapshot.team_id).unwrap().len() >= 4);
    }

    #[test]
    fn team_runtime_binds_spawned_agents_to_roles() {
        let service = TeamRuntimeService::new();
        let prompt = "research alternatives and synthesize a decision";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
        let mut spawned_roles = Vec::new();

        let snapshot = service
            .start_with_agent_spawner(
                StartTeamRuntimeRequest {
                    session_id: "session-team-bind".to_string(),
                    objective: prompt.to_string(),
                    collaboration_decision: decision,
                },
                |request| {
                    spawned_roles.push(request.role_id.clone());
                    Ok(fake_agent_snapshot(&request.role_id))
                },
            )
            .expect("team starts and binds agents");

        assert!(!spawned_roles.is_empty());
        assert!(snapshot.agents.iter().all(|agent| agent
            .agent_id
            .as_deref()
            .is_some_and(|id| id.starts_with("agent-"))));
        assert!(service
            .events(&snapshot.team_id)
            .expect("events")
            .iter()
            .any(|event| event.event_type == "team.agents_bound"));
    }

    #[test]
    fn team_runtime_cancel_propagates_to_bound_agents() {
        let service = TeamRuntimeService::new();
        let prompt = "implement and review a focused change";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
        let temp_root =
            std::env::temp_dir().join(format!("cowd-team-runtime-agents-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_root).expect("temp root");
        let mut spawned_ids = Vec::new();

        let snapshot = service
            .start_with_agent_spawner(
                StartTeamRuntimeRequest {
                    session_id: "session-team-cancel".to_string(),
                    objective: prompt.to_string(),
                    collaboration_decision: decision,
                },
                |request| {
                    let snapshot = fake_registered_agent_snapshot(&temp_root, &request.role_id);
                    spawned_ids.push(snapshot.agent_id.clone());
                    Ok(snapshot)
                },
            )
            .expect("team starts");

        service.cancel(&snapshot.team_id).expect("team cancel");
        for agent_id in spawned_ids {
            assert_eq!(
                global_agent_lifecycle_service()
                    .get(&agent_id)
                    .expect("agent")
                    .status,
                "cancel_requested"
            );
        }
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn team_runtime_marks_failed_when_agent_binding_fails() {
        let service = TeamRuntimeService::new();
        let prompt = "research and review a complex architecture";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);

        let result = service.start_with_agent_spawner(
            StartTeamRuntimeRequest {
                session_id: "session-team-bind-fails".to_string(),
                objective: prompt.to_string(),
                collaboration_decision: decision,
            },
            |_request| Err("spawner unavailable".to_string()),
        );

        assert!(result.is_err());
        let failed = service.projection()["teams"]
            .as_array()
            .expect("teams")
            .iter()
            .find(|team| team["session_id"] == "session-team-bind-fails")
            .cloned()
            .expect("failed team");
        assert_eq!(failed["status"], "failed");
    }

    #[test]
    fn collaboration_run_projection_exposes_agent_events_handoff_and_synthesis() {
        let service = TeamRuntimeService::new();
        let prompt = "research architecture, implement, review, and synthesize evidence";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
        let temp_root = std::env::temp_dir().join(format!(
            "cowd-collaboration-run-agents-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&temp_root).expect("temp root");

        let team = service
            .start_with_agent_spawner(
                StartTeamRuntimeRequest {
                    session_id: "session-collaboration-run".to_string(),
                    objective: prompt.to_string(),
                    collaboration_decision: decision,
                },
                |request| Ok(fake_registered_agent_snapshot(&temp_root, &request.role_id)),
            )
            .expect("team starts");

        let run = service.collaboration_run(&team.team_id).expect("run");
        assert_eq!(run.kind, "runtime.collaboration_run");
        assert_eq!(run.team.team_id, team.team_id);
        assert_eq!(run.agent_runs.len(), team.agents.len());
        assert!(run
            .agent_runs
            .iter()
            .all(|agent| !agent.lifecycle_events.is_empty()));
        assert!(run.control_actions.contains(&"synthesis".to_string()));

        service
            .handoff(
                &team.team_id,
                Some("human-agent".to_string()),
                Some("review synthesis before finalizing".to_string()),
            )
            .expect("handoff");
        let handed_off = service.collaboration_run(&team.team_id).expect("run");
        assert!(handed_off
            .team
            .review_notes
            .iter()
            .any(|note| note.contains("review synthesis")));
        assert!(handed_off
            .team_events
            .iter()
            .any(|event| event.event_type == "team.handoff_requested"));

        let summary = service
            .finalize_execution_summary(&team.team_id)
            .expect("summary");
        assert_eq!(summary.team_id, team.team_id);
        assert!(!summary.role_summaries.is_empty());
        assert!(summary
            .role_summaries
            .iter()
            .flat_map(|role| role.evidence_refs.iter())
            .any(|reference| reference.starts_with("team:")));
        let _ = std::fs::remove_dir_all(temp_root);
    }

    fn fake_agent_snapshot(role_id: &str) -> AgentSnapshot {
        AgentSnapshot {
            agent_id: format!("agent-{role_id}-{}", uuid::Uuid::new_v4()),
            name: role_id.to_string(),
            description: format!("agent for {role_id}"),
            subagent_type: Some("Explore".to_string()),
            model: Some(crate::DEFAULT_AGENT_MODEL.to_string()),
            status: "running".to_string(),
            backend: crate::AgentExecutionBackendKind::InProcess,
            output_file: String::new(),
            manifest_file: String::new(),
            created_at: "1".to_string(),
            started_at: Some("1".to_string()),
            completed_at: None,
            lane_events: Vec::new(),
            current_blocker: None,
            derived_state: "working".to_string(),
            error: None,
        }
    }

    fn fake_registered_agent_snapshot(root: &std::path::Path, role_id: &str) -> AgentSnapshot {
        let snapshot = AgentSnapshot {
            output_file: root.join(format!("{role_id}.md")).display().to_string(),
            manifest_file: root.join(format!("{role_id}.json")).display().to_string(),
            ..fake_agent_snapshot(role_id)
        };
        std::fs::write(&snapshot.output_file, "# Agent Task\n").expect("output");
        global_agent_lifecycle_service()
            .register_started(snapshot.clone(), crate::CancellationToken::new());
        snapshot
    }
}
