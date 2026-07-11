//! Steward autonomous supervisor scheduler.

use std::sync::{Mutex, OnceLock};

use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphCommand};
use serde::{Deserialize, Serialize};

use crate::{
    global_steward_runtime_service, MissionCommandExecutionReceipt, MissionCommandInterpreter,
    MissionControlRuntime, SessionDispatchMode, SessionExecutionPolicy, StewardLoopReport,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StewardSchedulerConfig {
    pub policy: StewardAutomationPolicy,
    pub max_session_commands_per_tick: usize,
    pub max_team_ticks: usize,
    pub allow_background_sessions: bool,
    #[serde(default = "default_dispatch_mode")]
    pub dispatch_mode: SessionDispatchMode,
}

impl Default for StewardSchedulerConfig {
    fn default() -> Self {
        let policy = StewardAutomationPolicy::Assisted;
        Self {
            max_session_commands_per_tick: policy.default_max_session_commands(),
            max_team_ticks: 10,
            allow_background_sessions: true,
            dispatch_mode: policy.default_dispatch_mode(),
            policy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StewardAutomationPolicy {
    Manual,
    Assisted,
    AutonomousBounded,
    YoloLikeButEvidenced,
}

impl Default for StewardAutomationPolicy {
    fn default() -> Self {
        Self::Assisted
    }
}

impl StewardAutomationPolicy {
    #[must_use]
    pub const fn default_dispatch_mode(self) -> SessionDispatchMode {
        match self {
            Self::Manual => SessionDispatchMode::MarkClaimedOnly,
            Self::Assisted | Self::AutonomousBounded | Self::YoloLikeButEvidenced => {
                SessionDispatchMode::StartRuntimeTurn
            }
        }
    }

    #[must_use]
    pub const fn default_max_session_commands(self) -> usize {
        match self {
            Self::Manual => 5,
            Self::Assisted => 10,
            Self::AutonomousBounded => 20,
            Self::YoloLikeButEvidenced => 50,
        }
    }

    #[must_use]
    pub fn spec(self) -> StewardAutomationPolicySpec {
        match self {
            Self::Manual => StewardAutomationPolicySpec {
                policy: self,
                can_dispatch_session_commands: true,
                allow_provider_calls: false,
                allow_start_agent_team: false,
                budget_label: "manual_claim_only".to_string(),
                approval_gate: "human_required".to_string(),
                default_dispatch_mode: SessionDispatchMode::MarkClaimedOnly,
            },
            Self::Assisted => StewardAutomationPolicySpec {
                policy: self,
                can_dispatch_session_commands: true,
                allow_provider_calls: true,
                allow_start_agent_team: false,
                budget_label: "bounded_assisted".to_string(),
                approval_gate: "approval_for_write_or_risk".to_string(),
                default_dispatch_mode: SessionDispatchMode::StartRuntimeTurn,
            },
            Self::AutonomousBounded => StewardAutomationPolicySpec {
                policy: self,
                can_dispatch_session_commands: true,
                allow_provider_calls: true,
                allow_start_agent_team: true,
                budget_label: "bounded_autonomy".to_string(),
                approval_gate: "policy_budget_and_risk_gate".to_string(),
                default_dispatch_mode: SessionDispatchMode::StartRuntimeTurn,
            },
            Self::YoloLikeButEvidenced => StewardAutomationPolicySpec {
                policy: self,
                can_dispatch_session_commands: true,
                allow_provider_calls: true,
                allow_start_agent_team: true,
                budget_label: "high_autonomy_evidence_required".to_string(),
                approval_gate: "evidence_required_sensitive_actions_gate".to_string(),
                default_dispatch_mode: SessionDispatchMode::StartRuntimeTurn,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardAutomationPolicySpec {
    pub policy: StewardAutomationPolicy,
    pub can_dispatch_session_commands: bool,
    pub allow_provider_calls: bool,
    pub allow_start_agent_team: bool,
    pub budget_label: String,
    pub approval_gate: String,
    pub default_dispatch_mode: SessionDispatchMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardSchedulerTickReport {
    pub kind: String,
    pub config: StewardSchedulerConfig,
    pub steward_loop: StewardLoopReport,
    pub session_dispatch: MissionCommandExecutionReceipt,
    pub team_submissions: Vec<StewardGraphSubmission>,
    pub ledger_records: Vec<StewardDecisionLedgerRecord>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardGraphSubmission {
    pub team_id: String,
    pub status: String,
    pub graph: ExecutionGraph,
    pub command: ExecutionGraphCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardDecisionLedgerRecord {
    pub record_id: String,
    pub steward_id: Option<String>,
    pub action: String,
    pub status: String,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardSchedulerProjection {
    pub kind: String,
    pub ledger_count: usize,
    pub latest: Vec<StewardDecisionLedgerRecord>,
    pub supported_dispatch_modes: Vec<SessionDispatchMode>,
    pub default_dispatch_mode: SessionDispatchMode,
    pub default_policy: StewardAutomationPolicy,
    pub policies: Vec<StewardAutomationPolicySpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardSchedulerHandoffSummary {
    pub kind: String,
    pub steward_id: String,
    pub completed: Vec<String>,
    pub pending: Vec<String>,
    pub blocked: Vec<String>,
    pub risk: Vec<String>,
    pub next_actions: Vec<String>,
    pub ledger: Vec<StewardDecisionLedgerRecord>,
}

#[derive(Debug, Default)]
pub struct StewardDecisionLedger {
    records: Mutex<Vec<StewardDecisionLedgerRecord>>,
}

impl StewardDecisionLedger {
    pub fn push(&self, mut record: StewardDecisionLedgerRecord) -> StewardDecisionLedgerRecord {
        if record.record_id.trim().is_empty() {
            record.record_id = format!("steward-ledger-{}", uuid::Uuid::new_v4());
        }
        if record.created_at_ms == 0 {
            record.created_at_ms = now_ms();
        }
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(record.clone());
        record
    }

    #[must_use]
    pub fn list(&self) -> Vec<StewardDecisionLedgerRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn list_for_steward(&self, steward_id: &str) -> Vec<StewardDecisionLedgerRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|record| record.steward_id.as_deref() == Some(steward_id))
            .cloned()
            .collect()
    }
}

pub fn global_steward_decision_ledger() -> &'static StewardDecisionLedger {
    static LEDGER: OnceLock<StewardDecisionLedger> = OnceLock::new();
    LEDGER.get_or_init(StewardDecisionLedger::default)
}

#[derive(Debug, Default)]
pub struct StewardScheduler;

impl StewardScheduler {
    pub fn tick(
        config: StewardSchedulerConfig,
        services: &crate::RuntimeServices,
    ) -> StewardSchedulerTickReport {
        let steward_loop = StewardLoopReport {
            kind: "runtime.steward_loop_report".to_string(),
            ticked: 0,
            skipped: global_steward_runtime_service().list().len(),
            decisions: Vec::new(),
            errors: vec!["capability_unavailable:steward_execution:V8".to_string()],
        };
        let mut ledger_records = Vec::new();
        for decision in &steward_loop.decisions {
            ledger_records.push(global_steward_decision_ledger().push(
                StewardDecisionLedgerRecord {
                    record_id: String::new(),
                    steward_id: Some(decision.steward_id.clone()),
                    action: decision.action.clone(),
                    status: format!("{:?}", decision.status).to_ascii_lowercase(),
                    summary: decision.reason.clone(),
                    evidence_refs: decision.evidence_refs.clone(),
                    created_at_ms: 0,
                },
            ));
        }

        let session_dispatch = MissionCommandInterpreter::prepare_submission(
            MissionCommandInterpreter::interpret_session_policy(SessionExecutionPolicy {
                max_commands: config.max_session_commands_per_tick,
                dispatch_mode: config.dispatch_mode,
                allow_background: config.allow_background_sessions,
            }),
        );
        ledger_records.push(
            global_steward_decision_ledger().push(StewardDecisionLedgerRecord {
                record_id: String::new(),
                steward_id: None,
                action: "session_dispatch_graph_submission".to_string(),
                status: "pending_runtime_host".to_string(),
                summary: "prepared SessionDispatch graph for RuntimeHost".to_string(),
                evidence_refs: Vec::new(),
                created_at_ms: 0,
            }),
        );

        let projection = MissionControlRuntime::projection(services);
        let mut team_submissions = Vec::new();
        let mut errors = steward_loop.errors.clone();
        for team in projection.teams.iter().take(config.max_team_ticks) {
            match services.graph_state_store().load(&team.graph_id) {
                Ok(graph) => {
                    let expected_revision = graph.revision;
                    ledger_records.push(global_steward_decision_ledger().push(
                        StewardDecisionLedgerRecord {
                            record_id: String::new(),
                            steward_id: None,
                            action: "team_execution_graph_submission".to_string(),
                            status: "capability_unavailable".to_string(),
                            summary: format!(
                                "observed canonical team {} graph; ExecutionGraphRunner owns advancement",
                                team.team_id
                            ),
                            evidence_refs: vec![format!(
                                "execution-graph:{}",
                                graph.id
                            )],
                            created_at_ms: 0,
                        },
                    ));
                    team_submissions.push(StewardGraphSubmission {
                        team_id: team.team_id.clone(),
                        status: "runner_owned".to_string(),
                        graph,
                        command: ExecutionGraphCommand::Start { expected_revision },
                    });
                }
                Err(error) => errors.push(format!("{}: {error}", team.team_id)),
            }
        }

        StewardSchedulerTickReport {
            kind: "runtime.steward_scheduler_tick_report".to_string(),
            config,
            steward_loop,
            session_dispatch,
            team_submissions,
            ledger_records,
            errors,
        }
    }

    #[must_use]
    pub fn projection() -> StewardSchedulerProjection {
        let mut latest = global_steward_decision_ledger().list();
        latest.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        StewardSchedulerProjection {
            kind: "runtime.steward_scheduler".to_string(),
            ledger_count: latest.len(),
            latest: latest.into_iter().take(20).collect(),
            supported_dispatch_modes: vec![
                SessionDispatchMode::MarkClaimedOnly,
                SessionDispatchMode::ControlDispatchComplete,
                SessionDispatchMode::StartRuntimeTurn,
            ],
            default_dispatch_mode: StewardAutomationPolicy::Assisted.default_dispatch_mode(),
            default_policy: StewardAutomationPolicy::Assisted,
            policies: vec![
                StewardAutomationPolicy::Manual.spec(),
                StewardAutomationPolicy::Assisted.spec(),
                StewardAutomationPolicy::AutonomousBounded.spec(),
                StewardAutomationPolicy::YoloLikeButEvidenced.spec(),
            ],
        }
    }

    #[must_use]
    pub fn handoff_summary(steward_id: &str) -> StewardSchedulerHandoffSummary {
        let ledger = global_steward_decision_ledger().list_for_steward(steward_id);
        let mut completed = Vec::new();
        let mut pending = Vec::new();
        let mut blocked = Vec::new();
        let mut risk = Vec::new();
        for record in &ledger {
            match record.status.as_str() {
                "delegated" | "executed" => completed.push(record.summary.clone()),
                "approvalsubmitted" | "approval_submitted" => pending.push(record.summary.clone()),
                "denied" | "blocked" | "failed" => blocked.push(record.summary.clone()),
                _ => risk.push(record.summary.clone()),
            }
        }
        StewardSchedulerHandoffSummary {
            kind: "runtime.steward_handoff_summary".to_string(),
            steward_id: steward_id.to_string(),
            completed,
            pending,
            blocked,
            risk,
            next_actions: vec![
                "review pending approvals".to_string(),
                "inspect blocked team or session commands".to_string(),
                "resume or takeover steward after review".to_string(),
            ],
            ledger,
        }
    }
}

const fn default_dispatch_mode() -> SessionDispatchMode {
    StewardAutomationPolicy::Assisted.default_dispatch_mode()
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
    use crate::{
        global_steward_runtime_service, AutonomyProfileId, StartMissionSessionRequest,
        StartStewardRuntimeRequest,
    };

    #[test]
    fn steward_scheduler_ticks_stewards_dispatches_and_records_ledger() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let _guard = crate::test_env_lock();
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("steward-scheduler-session-{suffix}");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "steward scheduler".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("session");
        let steward = global_steward_runtime_service()
            .start(StartStewardRuntimeRequest {
                mission_id: "scheduler-test".to_string(),
                root_session_id: Some(session_id),
                profile_id: AutonomyProfileId::Stewarded,
                objective: "supervise scheduler test".to_string(),
            })
            .expect("steward");

        let report = StewardScheduler::tick(StewardSchedulerConfig::default(), &services);
        assert_eq!(report.kind, "runtime.steward_scheduler_tick_report");
        assert_eq!(report.steward_loop.ticked, 0);
        assert_eq!(
            report.steward_loop.errors,
            vec!["capability_unavailable:steward_execution:V8"]
        );
        assert!(!report.ledger_records.is_empty());
        let handoff = StewardScheduler::handoff_summary(&steward.steward_id);
        assert_eq!(handoff.steward_id, steward.steward_id);
        assert!(!handoff.next_actions.is_empty());
    }

    #[test]
    fn steward_scheduler_submits_session_dispatch_graph_without_advancing_command() {
        let _guard = crate::test_env_lock();
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("steward-scheduler-turn-a-{suffix}");
        let session_b = format!("steward-scheduler-turn-b-{suffix}");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "steward scheduler turn a".to_string(),
                session_id: Some(session_a.clone()),
            })
            .expect("session a");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "steward scheduler turn b".to_string(),
                session_id: Some(session_b.clone()),
            })
            .expect("session b");
        let command = services
            .mission_runtime()
            .enqueue_session_command(&session_a, &session_b, "run deep background analysis")
            .expect("command");

        let report = StewardScheduler::tick(
            StewardSchedulerConfig {
                dispatch_mode: SessionDispatchMode::StartRuntimeTurn,
                ..StewardSchedulerConfig::default()
            },
            &services,
        );

        assert!(report.session_dispatch.ok);
        assert_eq!(
            report.session_dispatch.result["status"],
            "pending_runtime_host"
        );
        assert_eq!(
            report.session_dispatch.result["graph"]["nodes"][0]["kind"],
            "session_dispatch"
        );
        assert_eq!(
            services
                .mission_runtime()
                .get_session_command(&command.command_id)
                .expect("command after")
                .status,
            crate::MissionSessionCommandStatus::Pending
        );
    }

    #[test]
    fn steward_scheduler_default_policy_is_assisted_not_manual_claim_only() {
        let config = StewardSchedulerConfig::default();
        assert_eq!(config.policy, StewardAutomationPolicy::Assisted);
        assert_eq!(config.dispatch_mode, SessionDispatchMode::StartRuntimeTurn);
        let projection = StewardScheduler::projection();
        assert_eq!(
            projection.default_dispatch_mode,
            SessionDispatchMode::StartRuntimeTurn
        );
        assert!(projection
            .policies
            .iter()
            .any(|policy| policy.policy == StewardAutomationPolicy::Manual
                && policy.default_dispatch_mode == SessionDispatchMode::MarkClaimedOnly));
    }
}
