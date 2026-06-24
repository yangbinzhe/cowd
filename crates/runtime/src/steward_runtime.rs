//! Mission-level steward runtime.
//!
//! The steward runtime owns delegated supervision lifecycle. It evaluates
//! policy-bound actions through `StewardAgent`, records decisions, and exposes
//! a controllable entity to Gateway surfaces. It does not execute tools or
//! spawn agents directly.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use ai_kernel::core::TaskRisk;
use serde::{Deserialize, Serialize};

use crate::{
    record_runtime_event, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy,
    AutonomyProfileId, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, StewardActionRequest,
    StewardActionStatus, StewardAgent, StewardDecisionRecord,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StewardStatus {
    Created,
    Running,
    WaitingApproval,
    WaitingDependency,
    Paused,
    Completed,
    Failed,
    Cancelled,
    HandedOff,
}

impl StewardStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::WaitingApproval => "waiting_approval",
            Self::WaitingDependency => "waiting_dependency",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::HandedOff => "handed_off",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardSession {
    pub steward_id: String,
    pub mission_id: String,
    pub root_session_id: Option<String>,
    pub profile_id: AutonomyProfileId,
    pub status: StewardStatus,
    pub objective: String,
    pub active_team_ids: Vec<String>,
    pub active_agent_ids: Vec<String>,
    pub watched_session_ids: Vec<String>,
    pub pending_approval_ids: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardEvent {
    pub event_id: String,
    pub steward_id: String,
    pub mission_id: String,
    pub kind: String,
    pub summary: String,
    pub related_approval_id: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartStewardRuntimeRequest {
    pub mission_id: String,
    pub root_session_id: Option<String>,
    pub profile_id: AutonomyProfileId,
    pub objective: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickStewardRuntimeRequest {
    pub action: Option<String>,
    pub summary: Option<String>,
    pub risk: TaskRisk,
    pub requested_tool: Option<String>,
    pub requires_write: bool,
    pub is_critical_operation: bool,
    pub evidence_refs: Vec<String>,
    pub timeout_policy: ApprovalTimeoutPolicy,
}

impl Default for TickStewardRuntimeRequest {
    fn default() -> Self {
        Self {
            action: None,
            summary: None,
            risk: TaskRisk::Low,
            requested_tool: None,
            requires_write: false,
            is_critical_operation: false,
            evidence_refs: Vec::new(),
            timeout_policy: ApprovalTimeoutPolicy::Pending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardHandoffReport {
    pub steward_id: String,
    pub mission_id: String,
    pub status: StewardStatus,
    pub objective: String,
    pub decisions: Vec<StewardDecisionRecord>,
    pub pending_approval_ids: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub generated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardRuntimeProjection {
    pub kind: String,
    pub count: usize,
    pub running_count: usize,
    pub waiting_approval_count: usize,
    pub sessions: Vec<StewardSession>,
    pub events: Vec<StewardEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardLoopReport {
    pub kind: String,
    pub ticked: usize,
    pub skipped: usize,
    pub decisions: Vec<StewardDecisionRecord>,
    pub errors: Vec<String>,
}

#[derive(Debug, Default)]
pub struct StewardRuntimeService {
    sessions: Mutex<BTreeMap<String, StewardSession>>,
    decisions: Mutex<BTreeMap<String, Vec<StewardDecisionRecord>>>,
    events: Mutex<Vec<StewardEvent>>,
}

impl StewardRuntimeService {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(&self, request: StartStewardRuntimeRequest) -> Result<StewardSession, String> {
        if request.mission_id.trim().is_empty() {
            return Err("mission_id must not be empty".to_string());
        }
        if request.objective.trim().is_empty() {
            return Err("steward objective must not be empty".to_string());
        }
        let steward_id = format!("steward-{}", uuid::Uuid::new_v4());
        let now = now_ms();
        let session = StewardSession {
            steward_id: steward_id.clone(),
            mission_id: request.mission_id,
            root_session_id: request.root_session_id.clone(),
            profile_id: request.profile_id,
            status: StewardStatus::Running,
            objective: request.objective,
            active_team_ids: Vec::new(),
            active_agent_ids: Vec::new(),
            watched_session_ids: request.root_session_id.into_iter().collect(),
            pending_approval_ids: Vec::new(),
            evidence_refs: Vec::new(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(steward_id.clone(), session.clone());
        self.push_event(
            &session,
            "steward.started",
            format!(
                "steward started with profile {}",
                session.profile_id.as_str()
            ),
            None,
        );
        Ok(session)
    }

    pub fn list(&self) -> Vec<StewardSession> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    pub fn get(&self, steward_id: &str) -> Option<StewardSession> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(steward_id)
            .cloned()
    }

    pub fn tick(
        &self,
        steward_id: &str,
        request: TickStewardRuntimeRequest,
    ) -> Result<StewardDecisionRecord, String> {
        let session = self
            .get(steward_id)
            .ok_or_else(|| format!("steward not found: {steward_id}"))?;
        if matches!(
            session.status,
            StewardStatus::Paused
                | StewardStatus::Completed
                | StewardStatus::Cancelled
                | StewardStatus::HandedOff
        ) {
            return Err(format!(
                "steward {} cannot tick from {}",
                session.steward_id,
                session.status.as_str()
            ));
        }
        let action = request
            .action
            .unwrap_or_else(|| format!("advance objective: {}", session.objective));
        let summary = request.summary.unwrap_or_else(|| action.clone());
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Steward,
            session_id: session.root_session_id.clone(),
            agent_id: None,
            team_id: None,
            mission_id: Some(session.mission_id.clone()),
        };
        let record = StewardAgent::new().evaluate_action(StewardActionRequest {
            steward_id: session.steward_id.clone(),
            profile_id: session.profile_id,
            source,
            action,
            summary,
            risk: request.risk,
            requested_tool: request.requested_tool,
            template_id: None,
            requires_write: request.requires_write,
            is_critical_operation: request.is_critical_operation,
            evidence_refs: request.evidence_refs,
            timeout_policy: request.timeout_policy,
        })?;
        self.decisions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(steward_id.to_string())
            .or_default()
            .push(record.clone());
        self.update_after_decision(steward_id, &record)?;
        Ok(record)
    }

    pub fn tick_all_once(&self) -> StewardLoopReport {
        let sessions = self.list();
        let mut decisions = Vec::new();
        let mut errors = Vec::new();
        let mut skipped = 0usize;

        for session in sessions {
            if !matches!(
                session.status,
                StewardStatus::Running | StewardStatus::WaitingDependency
            ) {
                skipped = skipped.saturating_add(1);
                continue;
            }
            match self.tick(
                &session.steward_id,
                TickStewardRuntimeRequest {
                    action: Some(format!("watch mission {}", session.mission_id)),
                    summary: Some(format!(
                        "supervise objective and preserve evidence: {}",
                        session.objective
                    )),
                    risk: TaskRisk::Low,
                    requested_tool: Some("read_file".to_string()),
                    requires_write: false,
                    is_critical_operation: false,
                    evidence_refs: session.evidence_refs.clone(),
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            ) {
                Ok(decision) => decisions.push(decision),
                Err(error) => errors.push(format!("{}: {error}", session.steward_id)),
            }
        }

        StewardLoopReport {
            kind: "runtime.steward_loop_report".to_string(),
            ticked: decisions.len(),
            skipped,
            decisions,
            errors,
        }
    }

    pub fn pause(&self, steward_id: &str) -> Result<StewardSession, String> {
        self.set_status(steward_id, StewardStatus::Paused, "steward.paused")
    }

    pub fn resume(&self, steward_id: &str) -> Result<StewardSession, String> {
        self.set_status(steward_id, StewardStatus::Running, "steward.resumed")
    }

    pub fn interrupt(&self, steward_id: &str, reason: String) -> Result<StewardSession, String> {
        let session = self.set_status(steward_id, StewardStatus::Paused, "steward.interrupted")?;
        self.push_event(&session, "steward.interrupted", reason, None);
        Ok(session)
    }

    pub fn takeover(&self, steward_id: &str) -> Result<StewardHandoffReport, String> {
        let session =
            self.set_status(steward_id, StewardStatus::HandedOff, "steward.handed_off")?;
        Ok(self.report_for(&session))
    }

    pub fn mark_recovery_required(
        &self,
        steward_id: &str,
        reason: impl Into<String>,
    ) -> Result<StewardSession, String> {
        let reason = reason.into();
        let session = self.set_status(
            steward_id,
            StewardStatus::Paused,
            "steward.recovery_required",
        )?;
        self.push_event(&session, "steward.recovery_required", reason, None);
        Ok(session)
    }

    pub fn report(&self, steward_id: &str) -> Result<StewardHandoffReport, String> {
        let session = self
            .get(steward_id)
            .ok_or_else(|| format!("steward not found: {steward_id}"))?;
        Ok(self.report_for(&session))
    }

    pub fn projection(&self) -> StewardRuntimeProjection {
        let sessions = self.list();
        let running_count = sessions
            .iter()
            .filter(|session| session.status == StewardStatus::Running)
            .count();
        let waiting_approval_count = sessions
            .iter()
            .filter(|session| session.status == StewardStatus::WaitingApproval)
            .count();
        StewardRuntimeProjection {
            kind: "runtime.stewards".to_string(),
            count: sessions.len(),
            running_count,
            waiting_approval_count,
            sessions,
            events: self
                .events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    fn update_after_decision(
        &self,
        steward_id: &str,
        record: &StewardDecisionRecord,
    ) -> Result<(), String> {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = sessions
            .get_mut(steward_id)
            .ok_or_else(|| format!("steward not found: {steward_id}"))?;
        session.updated_at_ms = now_ms();
        session.evidence_refs.extend(record.evidence_refs.clone());
        session.evidence_refs.sort();
        session.evidence_refs.dedup();
        session.status = match record.status {
            StewardActionStatus::Delegated => StewardStatus::WaitingDependency,
            StewardActionStatus::ApprovalSubmitted => {
                if let Some(approval_id) = &record.approval_id {
                    session.pending_approval_ids.push(approval_id.clone());
                }
                StewardStatus::WaitingApproval
            }
            StewardActionStatus::Denied => StewardStatus::Paused,
        };
        let snapshot = session.clone();
        drop(sessions);
        self.push_event(
            &snapshot,
            "steward.evaluated_action",
            record.reason.clone(),
            record.approval_id.clone(),
        );
        Ok(())
    }

    fn set_status(
        &self,
        steward_id: &str,
        status: StewardStatus,
        event_kind: &str,
    ) -> Result<StewardSession, String> {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = sessions
            .get_mut(steward_id)
            .ok_or_else(|| format!("steward not found: {steward_id}"))?;
        session.status = status;
        session.updated_at_ms = now_ms();
        let snapshot = session.clone();
        drop(sessions);
        self.push_event(
            &snapshot,
            event_kind,
            format!("steward status {}", status.as_str()),
            None,
        );
        Ok(snapshot)
    }

    fn report_for(&self, session: &StewardSession) -> StewardHandoffReport {
        StewardHandoffReport {
            steward_id: session.steward_id.clone(),
            mission_id: session.mission_id.clone(),
            status: session.status,
            objective: session.objective.clone(),
            decisions: self
                .decisions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&session.steward_id)
                .cloned()
                .unwrap_or_default(),
            pending_approval_ids: session.pending_approval_ids.clone(),
            evidence_refs: session.evidence_refs.clone(),
            generated_at_ms: now_ms(),
        }
    }

    fn push_event(
        &self,
        session: &StewardSession,
        kind: impl Into<String>,
        summary: impl Into<String>,
        related_approval_id: Option<String>,
    ) {
        let kind = kind.into();
        let summary = summary.into();
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(StewardEvent {
                event_id: format!("steward-event-{}", uuid::Uuid::new_v4()),
                steward_id: session.steward_id.clone(),
                mission_id: session.mission_id.clone(),
                kind: kind.clone(),
                summary: summary.clone(),
                related_approval_id: related_approval_id.clone(),
                created_at_ms: now_ms(),
            });
        let refs = related_approval_id
            .into_iter()
            .map(|id| RuntimeEventRef {
                kind: "approval".to_string(),
                id,
            })
            .collect();
        let _ = record_runtime_event(RuntimeEventInput {
            stream_id: format!("steward:{}", session.steward_id),
            scope: RuntimeEventScope::Steward,
            kind,
            status: Some(session.status.as_str().to_string()),
            actor: Some("steward_runtime".to_string()),
            refs,
            payload: serde_json::json!({
                "summary": summary,
                "mission_id": session.mission_id,
                "root_session_id": session.root_session_id,
            }),
        });
    }
}

pub fn global_steward_runtime_service() -> &'static StewardRuntimeService {
    static SERVICE: OnceLock<StewardRuntimeService> = OnceLock::new();
    SERVICE.get_or_init(StewardRuntimeService::new)
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

    #[test]
    fn steward_runtime_tracks_lifecycle_and_handoff() {
        let runtime = StewardRuntimeService::new();
        let steward = runtime
            .start(StartStewardRuntimeRequest {
                mission_id: "mission-test".to_string(),
                root_session_id: Some("session-test".to_string()),
                profile_id: AutonomyProfileId::Stewarded,
                objective: "supervise implementation".to_string(),
            })
            .expect("start steward");

        let decision = runtime
            .tick(
                &steward.steward_id,
                TickStewardRuntimeRequest {
                    action: Some("read evidence".to_string()),
                    summary: Some("inspect evidence".to_string()),
                    risk: TaskRisk::Low,
                    requested_tool: Some("read_file".to_string()),
                    ..TickStewardRuntimeRequest::default()
                },
            )
            .expect("tick steward");
        assert_eq!(decision.status, StewardActionStatus::Delegated);

        let report = runtime
            .takeover(&steward.steward_id)
            .expect("takeover report");
        assert_eq!(report.status, StewardStatus::HandedOff);
        assert_eq!(report.decisions.len(), 1);
        assert_eq!(runtime.projection().count, 1);
    }

    #[test]
    fn steward_runtime_ticks_all_active_sessions_and_marks_recovery() {
        let runtime = StewardRuntimeService::new();
        let steward = runtime
            .start(StartStewardRuntimeRequest {
                mission_id: "mission-loop".to_string(),
                root_session_id: Some("session-loop".to_string()),
                profile_id: AutonomyProfileId::Stewarded,
                objective: "keep mission moving".to_string(),
            })
            .expect("start steward");

        let report = runtime.tick_all_once();
        assert_eq!(report.kind, "runtime.steward_loop_report");
        assert_eq!(report.ticked, 1);
        assert!(report.errors.is_empty());
        assert_eq!(
            runtime.get(&steward.steward_id).expect("steward").status,
            StewardStatus::WaitingDependency
        );

        let recovered = runtime
            .mark_recovery_required(&steward.steward_id, "gateway restart")
            .expect("mark recovery");
        assert_eq!(recovered.status, StewardStatus::Paused);
        assert!(runtime
            .projection()
            .events
            .iter()
            .any(|event| event.kind == "steward.recovery_required"));
    }
}
