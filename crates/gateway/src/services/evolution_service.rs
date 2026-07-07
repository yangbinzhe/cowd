use std::{fs, path::Path};

use serde::Deserialize;
use serde_json::{json, Value};

use super::{service_envelope, EvolutionService, ServiceEnvelope};

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
pub(crate) struct EvolutionProposalDecisionRequest {
    pub(crate) decision: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EvolutionSandboxEvalRequest {
    #[serde(default = "default_baseline_ref")]
    pub(crate) baseline_ref: String,
    #[serde(default = "default_candidate_ref")]
    pub(crate) candidate_ref: String,
    #[serde(default)]
    pub(crate) baseline_score: i32,
    #[serde(default = "default_candidate_score")]
    pub(crate) candidate_score: i32,
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
            owner: "0.9.462 Runtime evolution service boundary",
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

    pub(crate) fn create_proposal(
        &self,
        config_home: &Path,
        request: EvolutionProposalCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let signals = signal_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?;
        let selected = if request.signal_ids.is_empty() {
            signals.into_iter().take(3).collect::<Vec<_>>()
        } else {
            signals
                .into_iter()
                .filter(|signal| request.signal_ids.contains(&signal.signal_id))
                .collect::<Vec<_>>()
        };
        if selected.is_empty() {
            return Err(EvolutionServiceError::BadRequest(
                "at least one existing signal is required".to_string(),
            ));
        }
        let proposal = runtime::EvolutionProposal::from_signals(&selected);
        proposal_store(config_home)
            .append(&proposal)
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.proposal",
            "envelope": self.envelope("proposal_create"),
            "proposal": proposal,
            "plan_draft": proposal.to_plan_draft(),
        }))
    }

    pub(crate) fn proposal_detail(
        &self,
        config_home: &Path,
        id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = find_proposal(config_home, id)?;
        Ok(json!({
            "kind": "evolution.proposal_detail",
            "envelope": self.envelope("proposal_detail"),
            "proposal": proposal,
            "plan_draft": proposal.to_plan_draft(),
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

    pub(crate) fn sandbox_evals(&self, config_home: &Path) -> Result<Value, EvolutionServiceError> {
        let evals = sandbox_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.sandbox_evals",
            "envelope": self.envelope("sandbox_evals"),
            "count": evals.len(),
            "evals": evals,
        }))
    }

    pub(crate) fn start_sandbox_eval(
        &self,
        config_home: &Path,
        id: &str,
        request: EvolutionSandboxEvalRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = find_proposal(config_home, id)?;
        let artifact_root = evolution_root(config_home).join("sandbox-artifacts");
        fs::create_dir_all(&artifact_root)
            .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
        let artifact_path =
            artifact_root.join(format!("{}-sandbox-eval.json", proposal.proposal_id));
        let eval = runtime::EvolutionSandboxEval::compare(
            &proposal,
            request.baseline_ref,
            request.candidate_ref,
            artifact_path.display().to_string(),
            request.baseline_score,
            request.candidate_score,
        );
        fs::write(
            &artifact_path,
            serde_json::to_string_pretty(&eval)
                .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?,
        )
        .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
        sandbox_store(config_home)
            .append(&eval)
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.sandbox_eval",
            "envelope": self.envelope("sandbox_eval_start"),
            "eval": eval,
        }))
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.envelope("signals"),
            self.envelope("signal_create"),
            self.envelope("proposals"),
            self.envelope("proposal_create"),
            self.envelope("proposal_detail"),
            self.envelope("proposal_decision"),
            self.envelope("skill_draft"),
            self.envelope("sandbox_evals"),
            self.envelope("sandbox_eval_start"),
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

fn sandbox_store(config_home: &Path) -> runtime::EvolutionSandboxStore {
    runtime::EvolutionSandboxStore::new(evolution_root(config_home))
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

fn default_continue() -> bool {
    true
}

fn default_baseline_ref() -> String {
    "baseline:current".to_string()
}

fn default_candidate_ref() -> String {
    "candidate:sandbox".to_string()
}

fn default_candidate_score() -> i32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolution_service_links_signal_proposal_skill_and_sandbox() {
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
        let proposal_id = proposal["proposal"]["proposal_id"].as_str().unwrap();
        assert_eq!(proposal["plan_draft"]["blocked_mainline_write"], true);

        let draft = service
            .skill_draft(&config_home, proposal_id)
            .expect("draft");
        assert_eq!(draft["kind"], "evolution.skill_draft");
        assert!(draft["draft"]["markdown"]
            .as_str()
            .unwrap()
            .contains("Acceptance Gates"));

        let eval = service
            .start_sandbox_eval(
                &config_home,
                proposal_id,
                EvolutionSandboxEvalRequest {
                    baseline_ref: "baseline:main".to_string(),
                    candidate_ref: "candidate:worktree".to_string(),
                    baseline_score: 50,
                    candidate_score: 70,
                },
            )
            .expect("sandbox eval");
        assert_eq!(eval["eval"]["mainline_modified"], false);
        assert!(eval["eval"]["artifact_path"]
            .as_str()
            .unwrap()
            .contains("sandbox"));

        let decision = service
            .decide_proposal(
                &config_home,
                proposal_id,
                EvolutionProposalDecisionRequest {
                    decision: "approved".to_string(),
                },
            )
            .expect("decision");
        assert_eq!(decision["proposal"]["status"], "approved");
        assert_eq!(decision["mainline_modified"], false);

        let _ = fs::remove_dir_all(config_home);
    }
}
