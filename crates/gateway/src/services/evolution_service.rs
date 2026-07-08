use std::{collections::BTreeSet, fs, path::Path};

use serde::{Deserialize, Serialize};
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
pub(crate) struct EvolutionCandidateCreateRequest {
    #[serde(default = "default_baseline_ref")]
    pub(crate) baseline_ref: String,
    #[serde(default = "default_candidate_ref")]
    pub(crate) candidate_ref: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EvolutionCandidateDecisionRequest {
    pub(crate) status: runtime::EvolutionCandidateStatus,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EvolutionCandidateAdoptionRequest {
    #[serde(default = "default_adoption_status")]
    pub(crate) status: runtime::EvolutionCandidateStatus,
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
            owner: "0.9.465 Runtime evolution service boundary",
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
        let diagnoses = diagnosis_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.diagnoses",
            "envelope": self.envelope("diagnoses"),
            "store": diagnosis_store(config_home).path().display().to_string(),
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
                "candidate_count": mission.candidate_ids.len(),
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
        let candidates = candidate_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?
            .into_iter()
            .filter(|candidate| candidate.mission_id.as_deref() == Some(mission_id))
            .collect::<Vec<_>>();
        let candidate_ids = candidates
            .iter()
            .map(|candidate| candidate.candidate_id.clone())
            .collect::<BTreeSet<_>>();
        let comparisons =
            read_jsonl::<runtime::EvolutionComparisonReport>(&comparison_store_path(config_home))?
                .into_iter()
                .filter(|comparison| candidate_ids.contains(&comparison.candidate_id))
                .collect::<Vec<_>>();
        let promotions =
            read_jsonl::<runtime::EvolutionPromotionReceipt>(&promotion_store_path(config_home))?
                .into_iter()
                .filter(|promotion| candidate_ids.contains(&promotion.candidate_id))
                .collect::<Vec<_>>();
        let version_ids = promotions
            .iter()
            .filter_map(|promotion| promotion.version_record.as_ref())
            .map(|version| version.version_id.clone())
            .chain(
                candidates
                    .iter()
                    .filter_map(|candidate| candidate.version_record_ref.clone()),
            )
            .collect::<BTreeSet<_>>();
        let memories = read_jsonl::<runtime::EvolutionMemoryRecord>(&evolution_memory_store_path(
            config_home,
        ))?
        .into_iter()
        .filter(|memory| {
            candidate_ids.contains(&memory.candidate_id)
                || memory
                    .version_id
                    .as_ref()
                    .is_some_and(|version_id| version_ids.contains(version_id))
        })
        .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.mission_detail",
            "envelope": self.envelope("mission_detail"),
            "mission": mission,
            "proposals": proposals,
            "candidates": candidates,
            "comparisons": comparisons,
            "promotions": promotions,
            "memory": memories,
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
        let candidates = candidate_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?
            .into_iter()
            .filter(|candidate| candidate.proposal_id == proposal.proposal_id)
            .collect::<Vec<_>>();
        let evals = sandbox_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?
            .into_iter()
            .filter(|eval| eval.proposal_id == proposal.proposal_id)
            .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.chain",
            "envelope": self.envelope("chain"),
            "diagnosis": diagnosis,
            "proposal": proposal,
            "candidates": candidates,
            "sandbox_evals": evals,
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

    pub(crate) fn candidates(&self, config_home: &Path) -> Result<Value, EvolutionServiceError> {
        let candidates = candidate_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.candidates",
            "envelope": self.envelope("candidates"),
            "count": candidates.len(),
            "candidates": candidates,
        }))
    }

    pub(crate) fn create_candidate(
        &self,
        config_home: &Path,
        proposal_id: &str,
        request: EvolutionCandidateCreateRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let proposal = find_proposal(config_home, proposal_id)?;
        let artifact_root = evolution_root(config_home).join("candidate-artifacts");
        let candidate = runtime::EvolutionCandidateGenerator::generate_with_artifacts(
            &artifact_root,
            &proposal,
            request.baseline_ref,
            request.candidate_ref,
        )
        .map_err(EvolutionServiceError::Internal)?;
        candidate_store(config_home)
            .append(&candidate)
            .map_err(EvolutionServiceError::Internal)?;
        if let Some(mission_id) = candidate.mission_id.as_deref() {
            let _ = mission_store(config_home).update(mission_id, |mission| {
                mission.attach_candidate(candidate.candidate_id.clone())
            });
        }
        Ok(json!({
            "kind": "evolution.candidate",
            "envelope": self.envelope("candidate_create"),
            "candidate": candidate,
            "plan": candidate.plan(),
        }))
    }

    pub(crate) fn candidate_detail(
        &self,
        config_home: &Path,
        candidate_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let candidate = find_candidate(config_home, candidate_id)?;
        let evals = sandbox_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?
            .into_iter()
            .filter(|eval| eval.candidate_id.as_deref() == Some(candidate_id))
            .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.candidate_detail",
            "envelope": self.envelope("candidate_detail"),
            "candidate": candidate,
            "plan": candidate.plan(),
            "sandbox_evals": evals,
        }))
    }

    pub(crate) fn decide_candidate(
        &self,
        config_home: &Path,
        candidate_id: &str,
        request: EvolutionCandidateDecisionRequest,
    ) -> Result<Value, EvolutionServiceError> {
        if request.status == runtime::EvolutionCandidateStatus::ApprovedForAdoption {
            return self.adopt_candidate(
                config_home,
                candidate_id,
                EvolutionCandidateAdoptionRequest {
                    status: request.status,
                },
            );
        }
        let candidate = candidate_store(config_home)
            .update_status(candidate_id, request.status)
            .map_err(|error| {
                if error.contains("not found") {
                    EvolutionServiceError::NotFound(error)
                } else {
                    EvolutionServiceError::Internal(error)
                }
            })?;
        Ok(json!({
            "kind": "evolution.candidate_decision",
            "envelope": self.envelope("candidate_decision"),
            "candidate": candidate,
        }))
    }

    pub(crate) fn adopt_candidate(
        &self,
        config_home: &Path,
        candidate_id: &str,
        request: EvolutionCandidateAdoptionRequest,
    ) -> Result<Value, EvolutionServiceError> {
        let candidate = find_candidate(config_home, candidate_id)?;
        let passed_eval = passed_eval_for_candidate(config_home, candidate_id)?;
        let receipt = runtime::EvolutionAdoptionManager::evaluate(
            &candidate,
            request.status.clone(),
            passed_eval.as_ref().map(|eval| eval.eval_id.clone()),
        );
        if !receipt.accepted {
            return Err(EvolutionServiceError::BadRequest(receipt.reason));
        }
        let candidate = candidate_store(config_home)
            .update_status(candidate_id, request.status)
            .map_err(EvolutionServiceError::Internal)?;
        let receipt_path = evolution_root(config_home)
            .join("adoption-receipts")
            .join(format!("{}.json", receipt.receipt_id));
        if let Some(parent) = receipt_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
        }
        fs::write(
            &receipt_path,
            serde_json::to_string_pretty(&receipt)
                .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?,
        )
        .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
        Ok(json!({
            "kind": "evolution.candidate_adoption",
            "envelope": self.envelope("candidate_adoption"),
            "candidate": candidate,
            "receipt": receipt,
            "receipt_path": receipt_path.display().to_string(),
            "sandbox_eval": passed_eval,
        }))
    }

    pub(crate) fn run_candidate_sandbox(
        &self,
        config_home: &Path,
        candidate_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let candidate = find_candidate(config_home, candidate_id)?;
        let proposal = find_proposal(config_home, &candidate.proposal_id)?;
        let sandbox_root = evolution_root(config_home).join("runner");
        let policy = runtime::EvolutionRunnerPolicy::default();
        let runner = runtime::IsolatedRunner::new(&sandbox_root, policy);
        let runner_result = runner
            .run_artifact_check(&candidate)
            .map_err(EvolutionServiceError::Internal)?;
        append_jsonl(&runner_store_path(config_home), &runner_result)?;
        let eval = runtime::EvolutionSandboxOrchestrator::new(
            evolution_root(config_home).join("sandboxes"),
        )
        .run(&proposal, &candidate)
        .map_err(EvolutionServiceError::Internal)?;
        sandbox_store(config_home)
            .append(&eval)
            .map_err(EvolutionServiceError::Internal)?;
        let updated = candidate_store(config_home)
            .update_candidate(candidate_id, |candidate| {
                candidate.status = runtime::EvolutionCandidateStatus::Evaluated;
                candidate.artifact_root =
                    Some(sandbox_root.join(candidate_id).display().to_string());
                candidate.artifact_path = Some(eval.artifact_path.clone());
            })
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.sandbox_eval",
            "envelope": self.envelope("candidate_sandbox_run"),
            "candidate": updated,
            "runner_result": runner_result,
            "eval": eval,
        }))
    }

    pub(crate) fn candidate_artifacts(
        &self,
        config_home: &Path,
        candidate_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let candidate = find_candidate(config_home, candidate_id)?;
        Ok(json!({
            "kind": "evolution.candidate_artifacts",
            "envelope": self.envelope("candidate_artifacts"),
            "candidate_id": candidate_id,
            "artifact_root": candidate.artifact_root,
            "artifacts": candidate.generated_artifacts,
            "artifact_path": candidate.artifact_path,
        }))
    }

    pub(crate) fn evaluate_candidate(
        &self,
        config_home: &Path,
        candidate_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let candidate = find_candidate(config_home, candidate_id)?;
        let runner_results =
            read_jsonl::<runtime::EvolutionRunnerResult>(&runner_store_path(config_home))?;
        let runner_result = runner_results
            .iter()
            .rev()
            .find(|result| result.candidate_id == candidate_id);
        let request =
            runtime::EvolutionEvaluationRequest::from_candidate(&candidate, runner_result);
        let evidence_path = evolution_root(config_home)
            .join("comparisons")
            .join(format!("{}.json", request.request_id));
        if let Some(parent) = evidence_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
        }
        let runner_exit = runner_result.map(|result| result.exit_code).unwrap_or(0);
        let report = runtime::EvolutionComparisonReport::deterministic_from_request(
            &request,
            evidence_path.display().to_string(),
            runner_exit,
        );
        fs::write(
            &evidence_path,
            serde_json::to_string_pretty(&json!({"request": request, "report": report}))
                .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?,
        )
        .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
        append_jsonl(&comparison_store_path(config_home), &report)?;
        let updated = candidate_store(config_home)
            .update_candidate(candidate_id, |candidate| {
                candidate.comparison_report_ref = Some(report.comparison_id.clone());
                candidate.status = runtime::EvolutionCandidateStatus::Evaluated;
            })
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.candidate_comparison",
            "envelope": self.envelope("candidate_evaluate"),
            "candidate": updated,
            "comparison": report,
        }))
    }

    pub(crate) fn candidate_comparison(
        &self,
        config_home: &Path,
        candidate_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let reports =
            read_jsonl::<runtime::EvolutionComparisonReport>(&comparison_store_path(config_home))?
                .into_iter()
                .filter(|report| report.candidate_id == candidate_id)
                .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.candidate_comparison",
            "envelope": self.envelope("candidate_comparison"),
            "candidate_id": candidate_id,
            "count": reports.len(),
            "comparisons": reports,
        }))
    }

    pub(crate) fn promote_candidate(
        &self,
        config_home: &Path,
        candidate_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let candidate = find_candidate(config_home, candidate_id)?;
        let receipt = runtime::EvolutionPromotionManager::promote(&candidate);
        if !receipt.accepted {
            return Err(EvolutionServiceError::BadRequest(receipt.reason));
        }
        append_jsonl(&promotion_store_path(config_home), &receipt)?;
        if let Some(version) = receipt.version_record.as_ref() {
            append_jsonl(&version_store_path(config_home), version)?;
        }
        let memory = runtime::EvolutionMemoryBridge::from_promotion(&candidate, &receipt);
        append_jsonl(&evolution_memory_store_path(config_home), &memory)?;
        let updated = candidate_store(config_home)
            .update_candidate(candidate_id, |candidate| {
                candidate.status = runtime::EvolutionCandidateStatus::ApprovedForAdoption;
                candidate.version_record_ref = receipt
                    .version_record
                    .as_ref()
                    .map(|record| record.version_id.clone());
            })
            .map_err(EvolutionServiceError::Internal)?;
        Ok(json!({
            "kind": "evolution.candidate_promotion",
            "envelope": self.envelope("candidate_promote"),
            "candidate": updated,
            "promotion": receipt,
            "memory": memory,
        }))
    }

    pub(crate) fn adoptions(&self, config_home: &Path) -> Result<Value, EvolutionServiceError> {
        let promotions =
            read_jsonl::<runtime::EvolutionPromotionReceipt>(&promotion_store_path(config_home))?;
        Ok(json!({
            "kind": "evolution.adoptions",
            "envelope": self.envelope("adoptions"),
            "count": promotions.len(),
            "adoptions": promotions,
        }))
    }

    pub(crate) fn rollback_version(
        &self,
        config_home: &Path,
        version_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let versions =
            read_jsonl::<runtime::EvolutionVersionRecord>(&version_store_path(config_home))?;
        let version = versions
            .into_iter()
            .find(|version| version.version_id == version_id)
            .ok_or_else(|| {
                EvolutionServiceError::NotFound("evolution version not found".to_string())
            })?;
        let receipt = runtime::EvolutionRollbackManager::rollback(&version);
        append_jsonl(&rollback_store_path(config_home), &receipt)?;
        let memory = runtime::EvolutionMemoryBridge::from_rollback(&receipt);
        append_jsonl(&evolution_memory_store_path(config_home), &memory)?;
        Ok(json!({
            "kind": "evolution.rollback",
            "envelope": self.envelope("version_rollback"),
            "rollback": receipt,
            "memory": memory,
        }))
    }

    pub(crate) fn evolution_memory(
        &self,
        config_home: &Path,
    ) -> Result<Value, EvolutionServiceError> {
        let records = read_jsonl::<runtime::EvolutionMemoryRecord>(&evolution_memory_store_path(
            config_home,
        ))?;
        Ok(json!({
            "kind": "evolution.memory",
            "envelope": self.envelope("evolution_memory"),
            "count": records.len(),
            "records": records,
        }))
    }

    pub(crate) fn candidate_sandbox_eval(
        &self,
        config_home: &Path,
        candidate_id: &str,
    ) -> Result<Value, EvolutionServiceError> {
        let evals = sandbox_store(config_home)
            .list()
            .map_err(EvolutionServiceError::Internal)?
            .into_iter()
            .filter(|eval| eval.candidate_id.as_deref() == Some(candidate_id))
            .collect::<Vec<_>>();
        Ok(json!({
            "kind": "evolution.candidate_sandbox_eval",
            "envelope": self.envelope("candidate_sandbox_eval"),
            "candidate_id": candidate_id,
            "count": evals.len(),
            "evals": evals,
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
            self.envelope("candidates"),
            self.envelope("candidate_create"),
            self.envelope("candidate_detail"),
            self.envelope("candidate_decision"),
            self.envelope("candidate_promote"),
            self.envelope("adoptions"),
            self.envelope("version_rollback"),
            self.envelope("evolution_memory"),
            self.envelope("sandbox_evals"),
            self.envelope("candidate_sandbox_run"),
            self.envelope("candidate_sandbox_eval"),
            self.envelope("candidate_artifacts"),
            self.envelope("candidate_evaluate"),
            self.envelope("candidate_comparison"),
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

fn sandbox_store(config_home: &Path) -> runtime::EvolutionSandboxStore {
    runtime::EvolutionSandboxStore::new(evolution_root(config_home))
}

fn candidate_store(config_home: &Path) -> runtime::EvolutionCandidateStore {
    runtime::EvolutionCandidateStore::new(evolution_root(config_home))
}

fn runner_store_path(config_home: &Path) -> std::path::PathBuf {
    evolution_root(config_home).join("runner-results.jsonl")
}

fn comparison_store_path(config_home: &Path) -> std::path::PathBuf {
    evolution_root(config_home).join("comparison-reports.jsonl")
}

fn promotion_store_path(config_home: &Path) -> std::path::PathBuf {
    evolution_root(config_home).join("promotion-receipts.jsonl")
}

fn version_store_path(config_home: &Path) -> std::path::PathBuf {
    evolution_root(config_home).join("version-records.jsonl")
}

fn rollback_store_path(config_home: &Path) -> std::path::PathBuf {
    evolution_root(config_home).join("rollback-receipts.jsonl")
}

fn evolution_memory_store_path(config_home: &Path) -> std::path::PathBuf {
    evolution_root(config_home).join("evolution-memory.jsonl")
}

fn append_jsonl(path: &Path, value: &impl Serialize) -> Result<(), EvolutionServiceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
    }
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
    writeln!(
        file,
        "{}",
        serde_json::to_string(value)
            .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?
    )
    .map_err(|error| EvolutionServiceError::Internal(error.to_string()))
}

fn read_jsonl<T>(path: &Path) -> Result<Vec<T>, EvolutionServiceError>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    use std::io::{BufRead, BufReader};
    let file =
        fs::File::open(path).map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
    let mut records = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|error| EvolutionServiceError::Internal(error.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        records.push(
            serde_json::from_str(&line)
                .map_err(|error| EvolutionServiceError::Internal(error.to_string()))?,
        );
    }
    Ok(records)
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

fn find_candidate(
    config_home: &Path,
    id: &str,
) -> Result<runtime::EvolutionCandidate, EvolutionServiceError> {
    candidate_store(config_home)
        .find(id)
        .map_err(EvolutionServiceError::Internal)?
        .ok_or_else(|| EvolutionServiceError::NotFound("evolution candidate not found".to_string()))
}

fn passed_eval_for_candidate(
    config_home: &Path,
    id: &str,
) -> Result<Option<runtime::EvolutionSandboxEval>, EvolutionServiceError> {
    Ok(sandbox_store(config_home)
        .list()
        .map_err(EvolutionServiceError::Internal)?
        .into_iter()
        .find(|eval| {
            eval.candidate_id.as_deref() == Some(id)
                && eval.recommendation
                    == runtime::EvolutionSandboxRecommendation::AdoptAfterHumanApproval
                && !eval.mainline_modified
                && eval.regression_count == 0
        }))
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

fn default_adoption_status() -> runtime::EvolutionCandidateStatus {
    runtime::EvolutionCandidateStatus::ApprovedForAdoption
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
        assert_eq!(
            proposal["diagnosis"]["root_cause_kind"],
            "memory_governance_gap"
        );
        assert_eq!(proposal["plan_draft"]["blocked_mainline_write"], true);

        let draft = service
            .skill_draft(&config_home, proposal_id)
            .expect("draft");
        assert_eq!(draft["kind"], "evolution.skill_draft");
        assert!(draft["draft"]["markdown"]
            .as_str()
            .unwrap()
            .contains("Acceptance Gates"));

        let candidate = service
            .create_candidate(
                &config_home,
                proposal_id,
                EvolutionCandidateCreateRequest {
                    baseline_ref: "baseline:main".to_string(),
                    candidate_ref: "candidate:worktree".to_string(),
                },
            )
            .expect("candidate");
        let candidate_id = candidate["candidate"]["candidate_id"].as_str().unwrap();
        let run = service
            .run_candidate_sandbox(&config_home, candidate_id)
            .expect("candidate run");
        assert_eq!(run["eval"]["mainline_modified"], false);
        assert_eq!(run["eval"]["regression_count"], 0);
        assert!(run["runner_result"]["artifact_paths"].is_array());

        let comparison = service
            .evaluate_candidate(&config_home, candidate_id)
            .expect("comparison");
        assert_eq!(comparison["comparison"]["regression_count"], 0);

        let promotion = service
            .promote_candidate(&config_home, candidate_id)
            .expect("promotion");
        assert_eq!(promotion["promotion"]["accepted"], true);
        assert!(promotion["promotion"]["version_record"].is_object());
        let version_id = promotion["promotion"]["version_record"]["version_id"]
            .as_str()
            .expect("version id");

        let rollback = service
            .rollback_version(&config_home, version_id)
            .expect("rollback");
        assert_eq!(rollback["kind"], "evolution.rollback");
        assert_eq!(rollback["rollback"]["accepted"], true);
        assert_eq!(rollback["memory"]["kind"], "recovery_pattern");

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

    #[test]
    fn mission_detail_filters_comparisons_promotions_and_memory_by_mission() {
        let service = EvolutionService::new();
        let config_home =
            std::env::temp_dir().join(format!("cowd-evolution-filter-{}", uuid::Uuid::new_v4()));

        let first = create_candidate_chain(
            &service,
            &config_home,
            runtime::EvolutionSignalType::MemoryNoise,
            "first mission memory noise",
        );
        service
            .run_candidate_sandbox(&config_home, &first.candidate_id)
            .expect("first run");
        service
            .evaluate_candidate(&config_home, &first.candidate_id)
            .expect("first comparison");
        let promotion = service
            .promote_candidate(&config_home, &first.candidate_id)
            .expect("first promotion");
        let first_version_id = promotion["promotion"]["version_record"]["version_id"]
            .as_str()
            .expect("first version")
            .to_string();
        service
            .rollback_version(&config_home, &first_version_id)
            .expect("first rollback");

        let second = create_candidate_chain(
            &service,
            &config_home,
            runtime::EvolutionSignalType::EvalFailure,
            "second mission eval failure",
        );

        let first_detail = service
            .mission_detail(&config_home, &first.mission_id)
            .expect("first detail");
        assert_eq!(first_detail["proposals"].as_array().unwrap().len(), 1);
        assert_eq!(first_detail["candidates"].as_array().unwrap().len(), 1);
        assert_eq!(first_detail["comparisons"].as_array().unwrap().len(), 1);
        assert_eq!(first_detail["promotions"].as_array().unwrap().len(), 1);
        assert_eq!(first_detail["memory"].as_array().unwrap().len(), 2);
        assert!(first_detail["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| candidate["candidate_id"] == first.candidate_id));
        assert!(first_detail["memory"]
            .as_array()
            .unwrap()
            .iter()
            .all(|memory| memory["candidate_id"] == first.candidate_id));

        let second_detail = service
            .mission_detail(&config_home, &second.mission_id)
            .expect("second detail");
        assert_eq!(second_detail["proposals"].as_array().unwrap().len(), 1);
        assert_eq!(second_detail["candidates"].as_array().unwrap().len(), 1);
        assert_eq!(second_detail["comparisons"].as_array().unwrap().len(), 0);
        assert_eq!(second_detail["promotions"].as_array().unwrap().len(), 0);
        assert_eq!(second_detail["memory"].as_array().unwrap().len(), 0);
        assert_eq!(
            second_detail["candidates"][0]["candidate_id"],
            second.candidate_id
        );

        let _ = fs::remove_dir_all(config_home);
    }

    struct TestChainIds {
        mission_id: String,
        candidate_id: String,
    }

    fn create_candidate_chain(
        service: &EvolutionService,
        config_home: &Path,
        signal_type: runtime::EvolutionSignalType,
        summary: &str,
    ) -> TestChainIds {
        let signal = service
            .create_signal(
                config_home,
                EvolutionSignalCreateRequest {
                    signal_type,
                    source: runtime::EvolutionSignalSource {
                        owner: "runtime".to_string(),
                        session_id: Some("session-filter".to_string()),
                        agent_id: None,
                        team_id: None,
                        run_id: None,
                    },
                    evidence_refs: vec![format!("evidence:{summary}")],
                    severity: runtime::EvolutionSignalSeverity::Warning,
                    summary: summary.to_string(),
                    suggested_action: "create typed evolution candidate".to_string(),
                    immediate_task_can_continue: true,
                },
            )
            .expect("signal");
        let signal_id = signal["signal"]["signal_id"].as_str().unwrap().to_string();
        let proposal = service
            .create_proposal(
                config_home,
                EvolutionProposalCreateRequest {
                    signal_ids: vec![signal_id],
                },
            )
            .expect("proposal");
        let mission_id = proposal["proposal"]["mission_id"]
            .as_str()
            .expect("mission id")
            .to_string();
        let proposal_id = proposal["proposal"]["proposal_id"]
            .as_str()
            .expect("proposal id")
            .to_string();
        let candidate = service
            .create_candidate(
                config_home,
                &proposal_id,
                EvolutionCandidateCreateRequest {
                    baseline_ref: "baseline:test".to_string(),
                    candidate_ref: "candidate:test".to_string(),
                },
            )
            .expect("candidate");
        TestChainIds {
            mission_id,
            candidate_id: candidate["candidate"]["candidate_id"]
                .as_str()
                .expect("candidate id")
                .to_string(),
        }
    }
}
