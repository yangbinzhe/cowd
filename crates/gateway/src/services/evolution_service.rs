use std::path::Path;

use serde::Deserialize;
use serde_json::{json, Value};

use super::{service_envelope, EvolutionService, ServiceEnvelope};

/// Gateway accepts discovery signals and proposal drafts only. Definition
/// candidates, evaluation results and every release decision are Runtime-owned
/// aggregates exposed through the typed evolution routes.
#[derive(Debug, Deserialize)]
pub(crate) struct EvolutionSignalCreateRequest {
    pub(crate) signal_type: runtime::EvolutionSignalType,
    pub(crate) source: runtime::EvolutionSignalSource,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) severity: runtime::EvolutionSignalSeverity,
    pub(crate) summary: String,
    pub(crate) suggested_action: String,
    #[serde(default = "default_continue")]
    pub(crate) immediate_task_can_continue: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EvolutionProposalCreateRequest {
    #[serde(default)]
    pub(crate) signal_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvolutionProposalDecisionRequest {
    pub(crate) decision: String,
}

#[derive(Debug)]
pub(crate) enum EvolutionServiceError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl EvolutionServiceError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::BadRequest(message) | Self::NotFound(message) | Self::Internal(message) => {
                message.clone()
            }
        }
    }
}

impl EvolutionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "evolution",
            owner: "Gateway discovery facade; Runtime owns candidates, evaluation, and release",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn signals(&self, config_home: &Path) -> Result<Value, EvolutionServiceError> {
        let store = signal_store(config_home);
        let signals = store.list().map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.signals",
            "envelope": self.envelope("signals"),
            "store": store.path().display().to_string(),
            "count": signals.len(),
            "signals": signals,
        }))
    }

    pub(crate) fn create_signal(
        &self,
        config_home: &Path,
        request: EvolutionSignalCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        if request.summary.trim().is_empty() {
            return Err(EvolutionServiceError::BadRequest(
                "summary is required".to_string(),
            ));
        }
        let signal = runtime::EvolutionSignal::new(runtime::EvolutionSignalInput {
            signal_type: request.signal_type,
            source: request.source,
            evidence_refs: request.evidence_refs,
            severity: request.severity,
            summary: request.summary,
            suggested_action: request.suggested_action,
            immediate_task_can_continue: request.immediate_task_can_continue,
        });
        signal_store(config_home)
            .append(&signal)
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.signal",
            "envelope": self.envelope("signal_create"),
            "signal": signal,
        }))
    }

    pub(crate) fn proposals(&self, config_home: &Path) -> Result<Value, EvolutionServiceError> {
        let proposals = proposal_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.proposals",
            "envelope": self.envelope("proposals"),
            "count": proposals.len(),
            "proposals": proposals,
        }))
    }

    pub(crate) fn diagnoses(&self, config_home: &Path) -> Result<Value, EvolutionServiceError> {
        let store = diagnosis_store(config_home);
        let diagnoses = store.list().map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.diagnoses",
            "envelope": self.envelope("diagnoses"),
            "store": store.path().display().to_string(),
            "count": diagnoses.len(),
            "diagnoses": diagnoses,
        }))
    }

    pub(crate) fn mission_summary(
        &self,
        config_home: &Path,
    ) -> Result<Value, EvolutionServiceError> {
        let missions = mission_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.missions_summary",
            "envelope": self.envelope("missions_summary"),
            "count": missions.len(),
            "missions": missions.iter().map(|mission| json!({
                "mission_id": mission.mission_id,
                "status": mission.status,
                "owner": mission.owner,
                "scope": mission.scope,
                "goal_ids": mission.goal_ids,
                "signal_count": mission.signal_ids.len(),
                "proposal_count": mission.proposal_ids.len(),
                "updated_at_ms": mission.updated_at_ms,
            })).collect::<Vec<_>>(),
        }))
    }

    pub(crate) fn mission_detail(
        &self,
        config_home: &Path,
        mission_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let mission = mission_store(config_home)
            .find(mission_id)
            .map_err(EvolutionServiceError::Internal)?
            .ok_or_else(|| {
                EvolutionServiceError::NotFound("evolution mission not found".to_string())
            })?;
        let proposals = proposal_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?
            .into_iter()
            .filter(|proposal| proposal.mission_id.as_deref() == Some(mission_id))
            .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.mission_detail",
            "envelope": self.envelope("mission_detail"),
            "mission": mission,
            "proposals": proposals,
            "candidate_owner": "runtime",
            "candidate_query": "/api/evolution/candidates",
        }))
    }

    pub(crate) fn create_diagnosis(
        &self,
        config_home: &Path,
        request: EvolutionProposalCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let selected = select_signals(config_home, request.signal_ids)?;
        let diagnosis = runtime::EvolutionDiagnosisEngine::diagnose(&selected);
        diagnosis_store(config_home)
            .append(&diagnosis)
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.diagnosis",
            "envelope": self.envelope("diagnosis_create"),
            "diagnosis": diagnosis,
        }))
    }

    pub(crate) fn create_proposal(
        &self,
        config_home: &Path,
        request: EvolutionProposalCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let selected = select_signals(config_home, request.signal_ids)?;
        let mut drafts = runtime::EvolutionLifecycleService::open_from_signals(&selected);
        let draft = drafts.pop().ok_or_else(|| {
            EvolutionServiceError::BadRequest(
                "at least one existing signal is required".to_string(),
            )
        })?;
        let diagnosis = runtime::EvolutionDiagnosisEngine::diagnose(&selected);
        diagnosis_store(config_home)
            .append(&diagnosis)
            .map_err(EvolutionServiceError::Internal)?;
        mission_store(config_home)
            .append(&draft.mission)
            .map_err(EvolutionServiceError::Internal)?;
        let proposal = draft.proposal;
        proposal_store(config_home)
            .append(&proposal)
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.proposal",
            "envelope": self.envelope("proposal_create"),
            "diagnosis": diagnosis,
            "proposal": proposal,
            "plan_draft": proposal.to_plan_draft(),
            "candidate_owner": "runtime",
        }))
    }

    pub(crate) fn proposal_detail(
        &self,
        config_home: &Path,
        id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = find_proposal(config_home, id)?;
        let diagnosis = proposal
            .diagnosis_id
            .as_ref()
            .and_then(|diagnosis_id| find_diagnosis(config_home, diagnosis_id).ok().flatten());
        Ok(json!({
            "kind": "evolution.proposal_detail",
            "envelope": self.envelope("proposal_detail"),
            "diagnosis": diagnosis,
            "proposal": proposal,
            "plan_draft": proposal.to_plan_draft(),
            "candidate_owner": "runtime",
        }))
    }

    pub(crate) fn chain(
        &self,
        config_home: &Path,
        id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = find_proposal(config_home, id)?;
        let diagnosis = proposal
            .diagnosis_id
            .as_ref()
            .and_then(|diagnosis_id| find_diagnosis(config_home, diagnosis_id).ok().flatten());
        Ok(json!({
            "kind": "evolution.chain",
            "envelope": self.envelope("chain"),
            "diagnosis": diagnosis,
            "proposal": proposal,
            "candidate_owner": "runtime",
            "candidate_query": "/api/evolution/candidates",
        }))
    }

    pub(crate) fn proposal_model(
        &self,
        config_home: &Path,
        id: &str,
    ) -> Result<runtime::EvolutionProposal, EvolutionServiceError> {
        find_proposal(config_home, id)
    }

    pub(crate) fn decide_proposal(
        &self,
        config_home: &Path,
        id: &str,
        request: EvolutionProposalDecisionRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let decision = request.decision.trim();
        if !matches!(decision, "approved" | "rejected" | "archived") {
            return Err(EvolutionServiceError::BadRequest(
                "decision must be approved, rejected, or archived".to_string(),
            ));
        }
        let proposal = proposal_store(config_home)
            .update_status(id, decision)
            .map_err(|error| {
                if error.contains("not found") {
                    EvolutionServiceError::NotFound(error)
                } else {
                    EvolutionServiceError::Internal(error)
                }
            })?;
        Ok(json!({
            "kind": "evolution.proposal_decision",
            "envelope": self.envelope("proposal_decision"),
            "proposal": proposal,
            "mainline_modified": false,
        }))
    }

    pub(crate) fn skill_draft(
        &self,
        config_home: &Path,
        id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = find_proposal(config_home, id)?;
        let draft = proposal.to_skill_draft();
        Ok(json!({
            "kind": "evolution.skill_draft",
            "envelope": self.envelope("skill_draft"),
            "proposal_id": id,
            "draft": draft,
        }))
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.envelope("signals"),
            self.envelope("signal_create"),
            self.envelope("diagnoses"),
            self.envelope("diagnosis_create"),
            self.envelope("missions_summary"),
            self.envelope("mission_detail"),
            self.envelope("proposals"),
            self.envelope("proposal_create"),
            self.envelope("proposal_detail"),
            self.envelope("chain"),
            self.envelope("proposal_decision"),
            self.envelope("skill_draft"),
        ]
    }
}

fn evolution_root(config_home: &Path) -> std::path::PathBuf {
    config_home.join("evolution")
}

fn signal_store(config_home: &Path) -> runtime::EvolutionSignalStore {
    runtime::EvolutionSignalStore::new(evolution_root(config_home))
}

fn proposal_store(config_home: &Path) -> runtime::EvolutionProposalStore {
    runtime::EvolutionProposalStore::new(evolution_root(config_home))
}

fn diagnosis_store(config_home: &Path) -> runtime::EvolutionDiagnosisStore {
    runtime::EvolutionDiagnosisStore::new(evolution_root(config_home))
}

fn mission_store(config_home: &Path) -> runtime::EvolutionMissionStore {
    runtime::EvolutionMissionStore::new(evolution_root(config_home))
}

fn find_proposal(
    config_home: &Path,
    id: &str,
) -> Result<runtime::EvolutionProposal, EvolutionServiceError> {
    proposal_store(config_home)
        .list()
        .map_err(EvolutionServiceError::Internal)?
        .into_iter()
        .find(|proposal| proposal.proposal_id == id)
        .ok_or_else(|| EvolutionServiceError::NotFound("evolution proposal not found".to_string()))
}

fn select_signals(
    config_home: &Path,
    signal_ids: Vec<String>,
) -> Result<Vec<runtime::EvolutionSignal>, EvolutionServiceError> {
    let signals = signal_store(config_home)
        .list()
        .map_err(EvolutionServiceError::Internal)?;
    let selected = if signal_ids.is_empty() {
        signals.into_iter().take(3).collect::<Vec<_>>()
    } else {
        signals
            .into_iter()
            .filter(|signal| signal_ids.contains(&signal.signal_id))
            .collect::<Vec<_>>()
    };
    if selected.is_empty() {
        return Err(EvolutionServiceError::BadRequest(
            "at least one existing signal is required".to_string(),
        ));
    }
    Ok(selected)
}

fn find_diagnosis(
    config_home: &Path,
    id: &str,
) -> Result<Option<runtime::EvolutionDiagnosis>, EvolutionServiceError> {
    Ok(diagnosis_store(config_home)
        .list()
        .map_err(EvolutionServiceError::Internal)?
        .into_iter()
        .find(|diagnosis| diagnosis.diagnosis_id == id))
}

fn default_continue() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_facade_keeps_signal_proposal_and_skill_draft_outside_release_owner() {
        let service = EvolutionService::new();
        let config_home =
            std::env::temp_dir().join(format!("cowd-evolution-service-{}", uuid::Uuid::new_v4()));
        let signal = service
            .create_signal(
                &config_home,
                EvolutionSignalCreateRequest {
                    signal_type: runtime::EvolutionSignalType::MemoryNoise,
                    source: runtime::EvolutionSignalSource {
                        owner: "runtime".to_string(),
                        session_id: Some("session-1".to_string()),
                        agent_id: None,
                        team_id: None,
                        run_id: None,
                    },
                    evidence_refs: vec!["memory:packet:noise".to_string()],
                    severity: runtime::EvolutionSignalSeverity::Warning,
                    summary: "memory packet contained unrelated working memory".to_string(),
                    suggested_action: "tighten scope and salience gates".to_string(),
                    immediate_task_can_continue: true,
                },
            )
            .expect("signal");
        assert_eq!(signal["kind"], "evolution.signal");

        let proposal = service
            .create_proposal(
                &config_home,
                EvolutionProposalCreateRequest {
                    signal_ids: Vec::new(),
                },
            )
            .expect("proposal");
        let proposal_id = proposal["proposal"]["proposal_id"]
            .as_str()
            .expect("proposal id");
        assert_eq!(proposal["candidate_owner"], "runtime");
        assert_eq!(proposal["plan_draft"]["blocked_mainline_write"], true);

        let draft = service
            .skill_draft(&config_home, proposal_id)
            .expect("draft");
        assert!(draft["draft"]["markdown"]
            .as_str()
            .expect("markdown")
            .contains("Acceptance Gates"));

        let decision = service
            .decide_proposal(
                &config_home,
                proposal_id,
                EvolutionProposalDecisionRequest {
                    decision: "approved".to_string(),
                },
            )
            .expect("decision");
        assert_eq!(decision["mainline_modified"], false);
        assert!(!service.contracts().iter().any(|contract| contract
            .operation
            .contains("candidate")
            || contract.operation.contains("promotion")
            || contract.operation.contains("rollback")));

        let _ = std::fs::remove_dir_all(config_home);
    }
}
