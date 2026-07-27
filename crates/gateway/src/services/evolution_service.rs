use harness_contract::reality::EvidenceRef;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{service_envelope, EvolutionService, ServiceEnvelope};

/// Gateway accepts discovery commands and projects Runtime-owned evolution
/// state. It never opens an evolution registry or mutates release state.
#[derive(Debug, Deserialize)]
pub(crate) struct EvolutionSignalCreateRequest {
    pub(crate) signal_type: runtime::EvolutionSignalType,
    pub(crate) source: runtime::EvolutionSignalSource,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<EvidenceRef>,
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
            owner:
                "Gateway typed facade; Runtime owns discovery, candidates, evaluation, and release",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn signals(
        &self,
        runtime: &runtime::RuntimeServices,
    ) -> Result<Value, EvolutionServiceError> {
        let signals = runtime.evolution_signals().map_err(internal)?;
        let health = runtime.evolution_projector_health().map_err(internal)?;
        Ok(json!({
            "kind": "evolution.signals",
            "envelope": self.envelope("signals"),
            "count": signals.len(),
            "projector": health,
            "signals": signals,
        }))
    }

    pub(crate) fn create_signal(
        &self,
        runtime: &runtime::RuntimeServices,
        request: EvolutionSignalCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        if request.summary.trim().is_empty()
            || request.suggested_action.trim().is_empty()
            || request.evidence_refs.is_empty()
        {
            return Err(EvolutionServiceError::BadRequest(
                "summary, suggested action, and authoritative evidence are required".to_string(),
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
        let signal = runtime.record_evolution_signal(signal).map_err(internal)?;
        Ok(json!({
            "kind": "evolution.signal",
            "envelope": self.envelope("signal_create"),
            "signal": signal,
        }))
    }

    pub(crate) fn proposals(
        &self,
        runtime: &runtime::RuntimeServices,
    ) -> Result<Value, EvolutionServiceError> {
        let proposals = runtime.evolution_proposals().map_err(internal)?;
        Ok(json!({
            "kind": "evolution.proposals",
            "envelope": self.envelope("proposals"),
            "count": proposals.len(),
            "proposals": proposals,
        }))
    }

    pub(crate) fn diagnoses(
        &self,
        runtime: &runtime::RuntimeServices,
    ) -> Result<Value, EvolutionServiceError> {
        let diagnoses = runtime.evolution_diagnoses().map_err(internal)?;
        Ok(json!({
            "kind": "evolution.diagnoses",
            "envelope": self.envelope("diagnoses"),
            "count": diagnoses.len(),
            "diagnoses": diagnoses,
        }))
    }

    pub(crate) fn mission_summary(
        &self,
        runtime: &runtime::RuntimeServices,
    ) -> Result<Value, EvolutionServiceError> {
        let missions = runtime.evolution_missions().map_err(internal)?;
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
                "candidate_count": mission.candidate_ids.len(),
                "updated_at_ms": mission.updated_at_ms,
            })).collect::<Vec<_>>(),
        }))
    }

    pub(crate) fn mission_detail(
        &self,
        runtime: &runtime::RuntimeServices,
        mission_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let mission = runtime
            .evolution_mission(mission_id)
            .map_err(internal)?
            .ok_or_else(|| {
                EvolutionServiceError::NotFound("evolution mission not found".to_string())
            })?;
        let proposals = runtime
            .evolution_proposals()
            .map_err(internal)?
            .into_iter()
            .filter(|proposal| proposal.mission_id.as_deref() == Some(mission_id))
            .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.mission_detail",
            "envelope": self.envelope("mission_detail"),
            "mission": mission,
            "proposals": proposals,
            "candidate_owner": "runtime",
        }))
    }

    pub(crate) fn create_diagnosis(
        &self,
        runtime: &runtime::RuntimeServices,
        request: EvolutionProposalCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let diagnosis = runtime
            .create_evolution_diagnosis(request.signal_ids)
            .map_err(bad_or_internal)?;
        Ok(json!({
            "kind": "evolution.diagnosis",
            "envelope": self.envelope("diagnosis_create"),
            "diagnosis": diagnosis,
        }))
    }

    pub(crate) fn create_proposal(
        &self,
        runtime: &runtime::RuntimeServices,
        request: EvolutionProposalCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let draft = runtime
            .create_evolution_lifecycle(request.signal_ids)
            .map_err(bad_or_internal)?;
        let diagnosis = draft
            .proposal
            .diagnosis_id
            .as_deref()
            .and_then(|id| runtime.evolution_diagnosis(id).ok().flatten());
        let plan_draft = draft.proposal.to_plan_draft();
        Ok(json!({
            "kind": "evolution.proposal",
            "envelope": self.envelope("proposal_create"),
            "diagnosis": diagnosis,
            "proposal": draft.proposal,
            "plan_draft": plan_draft,
            "candidate_owner": "runtime",
        }))
    }

    pub(crate) fn proposal_detail(
        &self,
        runtime: &runtime::RuntimeServices,
        id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = self.proposal_model(runtime, id)?;
        let diagnosis = proposal
            .diagnosis_id
            .as_deref()
            .and_then(|diagnosis_id| runtime.evolution_diagnosis(diagnosis_id).ok().flatten());
        let plan_draft = proposal.to_plan_draft();
        Ok(json!({
            "kind": "evolution.proposal_detail",
            "envelope": self.envelope("proposal_detail"),
            "diagnosis": diagnosis,
            "proposal": proposal,
            "plan_draft": plan_draft,
            "candidate_owner": "runtime",
        }))
    }

    pub(crate) fn chain(
        &self,
        runtime: &runtime::RuntimeServices,
        id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = self.proposal_model(runtime, id)?;
        let diagnosis = proposal
            .diagnosis_id
            .as_deref()
            .and_then(|diagnosis_id| runtime.evolution_diagnosis(diagnosis_id).ok().flatten());
        let candidates = runtime
            .evolution_candidates()
            .map_err(internal)?
            .into_iter()
            .filter(|candidate| candidate.proposal_id == proposal.proposal_id)
            .collect::<Vec<_>>();
        let candidate_ids = candidates
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let reviews = runtime
            .evolution_release_reviews()
            .map_err(internal)?
            .into_iter()
            .filter(|review| {
                review
                    .candidate_id
                    .as_deref()
                    .is_some_and(|id| candidate_ids.contains(id))
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.chain",
            "envelope": self.envelope("chain"),
            "diagnosis": diagnosis,
            "proposal": proposal,
            "candidates": candidates,
            "reviews": reviews,
            "candidate_owner": "runtime",
        }))
    }

    pub(crate) fn proposal_model(
        &self,
        runtime: &runtime::RuntimeServices,
        id: &str,
    ) -> Result<runtime::EvolutionProposal, EvolutionServiceError> {
        runtime
            .evolution_proposal(id)
            .map_err(internal)?
            .ok_or_else(|| {
                EvolutionServiceError::NotFound("evolution proposal not found".to_string())
            })
    }

    pub(crate) fn skill_draft(
        &self,
        runtime: &runtime::RuntimeServices,
        id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = self.proposal_model(runtime, id)?;
        Ok(json!({
            "kind": "evolution.skill_draft",
            "envelope": self.envelope("skill_draft"),
            "proposal_id": id,
            "draft": proposal.to_skill_draft(),
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

fn internal(error: impl std::fmt::Display) -> EvolutionServiceError {
    EvolutionServiceError::Internal(error.to_string())
}

fn bad_or_internal(error: impl std::fmt::Display) -> EvolutionServiceError {
    let message = error.to_string();
    if message.contains("at least one existing") {
        EvolutionServiceError::BadRequest(message)
    } else {
        EvolutionServiceError::Internal(message)
    }
}

fn default_continue() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_facade_projects_the_runtime_event_store() {
        let service = EvolutionService::new();
        let runtime = runtime::RuntimeServices::in_memory().expect("runtime");
        let signal = service
            .create_signal(
                &runtime,
                EvolutionSignalCreateRequest {
                    signal_type: runtime::EvolutionSignalType::MemoryNoise,
                    source: runtime::EvolutionSignalSource {
                        owner: "runtime".to_string(),
                        session_id: Some("session-1".to_string()),
                        agent_id: None,
                        team_id: None,
                        run_id: None,
                    },
                    evidence_refs: vec![EvidenceRef::new("memory", "packet:noise")],
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
                &runtime,
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
        assert_eq!(service.proposals(&runtime).expect("proposals")["count"], 1);
        assert_eq!(
            service.skill_draft(&runtime, proposal_id).expect("draft")["proposal_id"],
            proposal_id
        );
    }
}
