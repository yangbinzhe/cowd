//! Runtime-owned team lifecycle service.
//!
//! TeamRuntime consumes a `CollaborationDecision` and turns it into a concrete
//! runtime team projection. It owns team status, role agents, events, and human
//! control commands; execution backends can attach to this service without
//! redefining team state.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::{
    global_agent_lifecycle_service, global_runtime_control_plane, AgentSnapshot,
    CollaborationDecision, CollaborationPlan, CollaborationTemplateId,
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
        self.runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(team_id)
            .map(|record| record.snapshot.clone())
    }

    #[must_use]
    pub fn list(&self) -> Vec<TeamRuntimeSnapshot> {
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
            record.touch();
            record.push_event("team.completed", "team merge completed");
            "merge completed".to_string()
        })
    }

    pub fn projection(&self) -> serde_json::Value {
        let teams = self.list();
        serde_json::json!({
            "kind": "runtime.teams",
            "count": teams.len(),
            "teams": teams,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CollaborationTemplateMatcher;
    use ai_kernel::strategy::{decide_strategy, StrategyInput};

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
