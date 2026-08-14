//! Sole durable write boundary for execution outcomes and explicit imports.

use std::path::Path;
use std::sync::Arc;

use harness_contract::outcome::{ExecutionOutcome, RuntimeBuildIdentity, OUTCOME_SCHEMA_REVISION};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};

pub const OUTCOME_EVENT_KIND: &str = "runtime.outcome.recorded.v1";
pub const OUTCOME_IMPORT_EVENT_KIND: &str = "runtime.outcome.legacy_imported.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeRecordReceipt {
    pub execution_id: String,
    pub terminal_generation: u64,
    pub commit_cursor: u64,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyOutcomeImportReceipt {
    pub source_sha256: String,
    pub record_count: usize,
    pub accepted_count: usize,
    pub rejected_count: usize,
    pub commit_cursor: u64,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationOutcomeImportReceipt {
    pub source_sha256: String,
    pub evaluator: String,
    pub evaluator_version: String,
    pub record_count: usize,
    pub accepted_execution_ids: Vec<String>,
    pub rejected_records: Vec<String>,
    pub commit_cursor: u64,
    pub duplicate: bool,
}

#[derive(Debug)]
pub struct OutcomeService {
    event_store: Arc<RuntimeEventStore>,
    runtime_build_identity: RuntimeBuildIdentity,
}

impl OutcomeService {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self::with_build_identity(
            event_store,
            RuntimeBuildIdentity::unresolved_development(env!("CARGO_PKG_VERSION")),
        )
    }

    /// Construct the sole durable Outcome writer with the immutable identity
    /// selected by the process composition root.
    #[must_use]
    pub fn with_build_identity(
        event_store: Arc<RuntimeEventStore>,
        runtime_build_identity: RuntimeBuildIdentity,
    ) -> Self {
        Self {
            event_store,
            runtime_build_identity,
        }
    }

    #[must_use]
    pub fn runtime_build_identity(&self) -> &RuntimeBuildIdentity {
        &self.runtime_build_identity
    }

    pub fn record_terminal(
        &self,
        outcome: &ExecutionOutcome,
    ) -> Result<OutcomeRecordReceipt, String> {
        self.runtime_build_identity.validate_for_recording()?;
        // Producers own workspace/config facts, not executable provenance.
        // Canonicalize before idempotency comparison and append so root,
        // delegated Agent, and Team outcomes cannot diverge or forge identity.
        let mut canonical_outcome = outcome.clone();
        canonical_outcome.runtime.runtime_revision = self.runtime_build_identity.semver.clone();
        canonical_outcome.runtime.build = self.runtime_build_identity.clone();
        let outcome = &canonical_outcome;
        if outcome.schema_revision != OUTCOME_SCHEMA_REVISION {
            return Err(format!(
                "outcome schema revision {} is unsupported",
                outcome.schema_revision
            ));
        }
        if outcome.identity.execution_id.trim().is_empty()
            || outcome.identity.session_id.trim().is_empty()
            || outcome.identity.turn_id.trim().is_empty()
        {
            return Err("outcome identity is incomplete".to_string());
        }
        validate_strategy_feedback(outcome)?;
        let stream_id = format!("outcome:{}", outcome.identity.execution_id);
        let key = format!(
            "{}:{}:{}",
            outcome.identity.execution_id,
            outcome.identity.terminal_generation,
            outcome.schema_revision
        );
        if let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &key)
            .map_err(|error| error.to_string())?
        {
            ensure_same_outcome(&existing.payload, outcome)?;
            return Ok(OutcomeRecordReceipt {
                execution_id: outcome.identity.execution_id.clone(),
                terminal_generation: outcome.identity.terminal_generation,
                commit_cursor: existing.commit_cursor,
                duplicate: true,
            });
        }
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let receipt = match self.event_store.append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!("runtime-outcome:{key}"),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: outcome_scope(outcome),
                    kind: OUTCOME_EVENT_KIND.to_string(),
                    status: Some(outcome.terminal.class_name().to_string()),
                    actor: Some("runtime.outcome_service".to_string()),
                    refs: outcome_refs(outcome),
                    payload: serde_json::to_value(outcome).map_err(|error| error.to_string())?,
                },
                idempotency_key: Some(key.clone()),
                schema_version: OUTCOME_SCHEMA_REVISION,
            }],
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Some(existing) = self
                    .event_store
                    .event_by_idempotency_key(&stream_id, &key)
                    .map_err(|lookup_error| lookup_error.to_string())?
                {
                    ensure_same_outcome(&existing.payload, outcome)?;
                    return Ok(OutcomeRecordReceipt {
                        execution_id: outcome.identity.execution_id.clone(),
                        terminal_generation: outcome.identity.terminal_generation,
                        commit_cursor: existing.commit_cursor,
                        duplicate: true,
                    });
                }
                return Err(error.to_string());
            }
        };
        Ok(OutcomeRecordReceipt {
            execution_id: outcome.identity.execution_id.clone(),
            terminal_generation: outcome.identity.terminal_generation,
            commit_cursor: receipt.commit_cursor,
            duplicate: receipt.duplicate,
        })
    }

    /// Import an old strategy experience file as provenance-only evidence.
    ///
    /// Legacy records lack complete provider/config identities and therefore
    /// never enter online routing segments. The explicit receipt preserves
    /// their audit value without restoring the retired hot-path file owner.
    pub fn import_legacy_strategy_file(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<LegacyOutcomeImportReceipt, String> {
        let bytes = std::fs::read(path.as_ref()).map_err(|error| error.to_string())?;
        let source_sha256 = format!("{:x}", Sha256::digest(&bytes));
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        let records = value
            .get("records")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "legacy strategy file has no records array".to_string())?;
        let accepted_count = records.iter().filter(|record| record.is_object()).count();
        let rejected_count = records.len().saturating_sub(accepted_count);
        let stream_id = "outcome-legacy-imports".to_string();
        let key = format!("legacy-import:{source_sha256}");
        if let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &key)
            .map_err(|error| error.to_string())?
        {
            return Ok(LegacyOutcomeImportReceipt {
                source_sha256,
                record_count: records.len(),
                accepted_count,
                rejected_count,
                commit_cursor: existing.commit_cursor,
                duplicate: true,
            });
        }
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let receipt = self
            .event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("runtime-outcome-import:{source_sha256}"),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: RuntimeEventScope::Recovery,
                        kind: OUTCOME_IMPORT_EVENT_KIND.to_string(),
                        status: Some("imported".to_string()),
                        actor: Some("runtime.outcome_service".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({
                            "source_path": path.as_ref().display().to_string(),
                            "source_sha256": source_sha256,
                            "record_count": records.len(),
                            "accepted_count": accepted_count,
                            "rejected_count": rejected_count,
                            "records": records,
                        }),
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(LegacyOutcomeImportReceipt {
            source_sha256,
            record_count: records.len(),
            accepted_count,
            rejected_count,
            commit_cursor: receipt.commit_cursor,
            duplicate: receipt.duplicate,
        })
    }

    /// Explicitly import fully typed paired calibration Outcomes.
    ///
    /// The command is maintenance-only: ordinary Turns never inspect files or
    /// environment variables. Rejected records remain in the receipt and
    /// cannot enter the online Outcome projection.
    pub fn import_calibration_file(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<CalibrationOutcomeImportReceipt, String> {
        let bytes = std::fs::read(path.as_ref()).map_err(|error| error.to_string())?;
        let source_sha256 = format!("{:x}", Sha256::digest(&bytes));
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        let evaluator = required_string(&value, "evaluator")?;
        let evaluator_version = required_string(&value, "evaluator_version")?;
        let records = value
            .get("outcomes")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "calibration file has no outcomes array".to_string())?;
        let stream_id = "outcome-calibration-imports".to_string();
        let key = format!("calibration-import:{source_sha256}");
        if let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &key)
            .map_err(|error| error.to_string())?
        {
            let mut receipt = existing
                .payload
                .get("receipt")
                .cloned()
                .ok_or_else(|| "calibration import receipt payload is missing".to_string())
                .and_then(|payload| {
                    serde_json::from_value::<CalibrationOutcomeImportReceipt>(payload)
                        .map_err(|error| error.to_string())
                })?;
            receipt.duplicate = true;
            receipt.commit_cursor = existing.commit_cursor;
            return Ok(receipt);
        }

        let mut accepted = Vec::new();
        let mut rejected_records = Vec::new();
        for (index, record) in records.iter().enumerate() {
            let outcome = match serde_json::from_value::<ExecutionOutcome>(record.clone()) {
                Ok(outcome) => outcome,
                Err(error) => {
                    rejected_records.push(format!("record[{index}]: invalid outcome: {error}"));
                    continue;
                }
            };
            if let Err(error) = validate_calibration_outcome(&outcome) {
                rejected_records.push(format!("record[{index}]: {error}"));
                continue;
            }
            self.record_terminal(&outcome)?;
            accepted.push(outcome.identity.execution_id);
        }
        let provisional = CalibrationOutcomeImportReceipt {
            source_sha256: source_sha256.clone(),
            evaluator,
            evaluator_version,
            record_count: records.len(),
            accepted_execution_ids: accepted,
            rejected_records,
            commit_cursor: 0,
            duplicate: false,
        };
        let receipt = self
            .event_store
            .append_batch_if_revision(
                stream_id.clone(),
                self.event_store
                    .stream_revision(&stream_id)
                    .map_err(|error| error.to_string())?,
                format!("runtime-outcome-calibration-import:{source_sha256}"),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: RuntimeEventScope::Recovery,
                        kind: "runtime.outcome.calibration_imported.v1".to_string(),
                        status: Some("imported".to_string()),
                        actor: Some("runtime.outcome_service".to_string()),
                        refs: provisional
                            .accepted_execution_ids
                            .iter()
                            .map(|id| RuntimeEventRef {
                                kind: "execution".to_string(),
                                id: id.clone(),
                            })
                            .collect(),
                        payload: serde_json::json!({
                            "source_path": path.as_ref().display().to_string(),
                            "receipt": provisional.clone(),
                        }),
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(CalibrationOutcomeImportReceipt {
            commit_cursor: receipt.commit_cursor,
            ..provisional
        })
    }
}

fn ensure_same_outcome(
    existing_payload: &serde_json::Value,
    expected: &ExecutionOutcome,
) -> Result<(), String> {
    let existing = serde_json::from_value::<ExecutionOutcome>(existing_payload.clone())
        .map_err(|error| format!("existing Outcome payload is invalid: {error}"))?;
    if &existing != expected {
        return Err(format!(
            "outcome idempotency conflict for execution {} generation {}",
            expected.identity.execution_id, expected.identity.terminal_generation
        ));
    }
    Ok(())
}

fn required_string(value: &serde_json::Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("calibration file requires `{field}`"))
}

fn validate_calibration_outcome(outcome: &ExecutionOutcome) -> Result<(), String> {
    if outcome.schema_revision != OUTCOME_SCHEMA_REVISION {
        return Err("unsupported Outcome schema revision".to_string());
    }
    if outcome
        .identity
        .paired_sample_id
        .as_deref()
        .is_none_or(|id| id.trim().is_empty())
    {
        return Err("paired_sample_id is required".to_string());
    }
    let provider = outcome
        .provider
        .as_ref()
        .ok_or_else(|| "provider identity is required".to_string())?;
    if provider.registry_revision.is_none()
        || provider.provider_name.trim().is_empty()
        || provider.model.trim().is_empty()
        || provider.profile.as_deref().is_none_or(str::is_empty)
        || provider.protocol.as_deref().is_none_or(str::is_empty)
    {
        return Err("complete provider identity is required".to_string());
    }
    match &outcome.quality {
        harness_contract::outcome::OutcomeQuality::Estimate {
            calibration_ref: Some(calibration_ref),
            ..
        } if !calibration_ref.trim().is_empty() => {}
        _ => return Err("calibrated quality evidence is required".to_string()),
    }
    if !matches!(
        outcome.evidence_completeness,
        harness_contract::reality::EvidenceCompleteness::Sufficient
    ) || outcome.evidence_refs.is_empty()
    {
        return Err("sufficient durable evidence is required".to_string());
    }
    if outcome.strategy_feedback.workload.is_none() {
        return Err("calibration workload fingerprint is required".to_string());
    }
    if !matches!(
        outcome.strategy_feedback.evaluation_environment.as_str(),
        "harness_evaluation" | "evolution_evaluation"
    ) {
        return Err("calibration evaluation environment is invalid".to_string());
    }
    Ok(())
}

fn validate_strategy_feedback(outcome: &ExecutionOutcome) -> Result<(), String> {
    let Some(workload) = outcome.strategy_feedback.workload.as_ref() else {
        return Ok(());
    };
    if outcome.runtime.workspace_key.trim().is_empty()
        || outcome.runtime.config_revision.trim().is_empty()
    {
        return Err("scoped strategy feedback requires workspace and config revision".to_string());
    }
    if outcome.provider.as_ref().is_none_or(|provider| {
        provider.provider_name.trim().is_empty() || provider.model.trim().is_empty()
    }) {
        return Err("scoped strategy feedback requires provider and model".to_string());
    }
    if workload.responsibility_domains == 0
        || !matches!(
            workload.tool_dag_shape.as_str(),
            "mixed_read_serial_write"
                | "bounded_serial_write"
                | "parallel_idempotent_read"
                | "direct_read_or_reason"
        )
    {
        return Err("strategy workload fingerprint is invalid".to_string());
    }
    if !matches!(
        outcome.strategy_feedback.evaluation_environment.as_str(),
        "production" | "harness_evaluation" | "evolution_evaluation"
    ) {
        return Err("strategy feedback evaluation environment is invalid".to_string());
    }
    Ok(())
}

fn outcome_refs(outcome: &ExecutionOutcome) -> Vec<RuntimeEventRef> {
    let mut refs = vec![
        RuntimeEventRef {
            kind: "execution".to_string(),
            id: outcome.identity.execution_id.clone(),
        },
        RuntimeEventRef {
            kind: "session".to_string(),
            id: outcome.identity.session_id.clone(),
        },
        RuntimeEventRef {
            kind: "turn".to_string(),
            id: outcome.identity.turn_id.clone(),
        },
        RuntimeEventRef {
            kind: "strategy_decision".to_string(),
            id: outcome.strategy.decision_id.clone(),
        },
    ];
    for (kind, id) in [
        ("task", outcome.identity.task_id.as_ref()),
        ("mission", outcome.identity.mission_id.as_ref()),
        ("agent", outcome.identity.agent_id.as_ref()),
        ("team", outcome.identity.team_id.as_ref()),
        (
            "execution_graph",
            outcome.identity.execution_graph_ref.as_ref(),
        ),
    ] {
        if let Some(id) = id {
            refs.push(RuntimeEventRef {
                kind: kind.to_string(),
                id: id.clone(),
            });
        }
    }
    refs
}

fn outcome_scope(outcome: &ExecutionOutcome) -> RuntimeEventScope {
    if outcome.identity.agent_id.is_some() {
        RuntimeEventScope::Agent
    } else if outcome.identity.team_id.is_some() {
        RuntimeEventScope::Team
    } else if outcome.strategy.selected_candidate
        == harness_contract::strategy::ExecutionCandidateKind::ParallelTools
    {
        RuntimeEventScope::Tool
    } else {
        RuntimeEventScope::Task
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{
        outcome::{
            OutcomeIdentity, OutcomeObservation, OutcomeQuality, OutcomeTerminalClass,
            OutcomeTiming, OutcomeUsage, ProviderIdentity, RuntimeBuildIdentity, RuntimeIdentity,
            StrategyIdentity,
        },
        reality::EvidenceCompleteness,
        strategy::ExecutionCandidateKind,
    };

    fn outcome(candidate: ExecutionCandidateKind) -> ExecutionOutcome {
        ExecutionOutcome {
            identity: OutcomeIdentity {
                execution_id: format!("execution-{candidate:?}"),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                terminal_generation: 1,
                paired_sample_id: None,
                task_id: None,
                mission_id: None,
                agent_id: None,
                team_id: None,
                execution_graph_ref: None,
            },
            runtime: RuntimeIdentity {
                workspace_key: "workspace".to_string(),
                runtime_revision: "test".to_string(),
                config_revision: "cfg".to_string(),
                build: Default::default(),
            },
            provider: Some(ProviderIdentity {
                registry_revision: Some(1),
                provider_name: "provider".to_string(),
                model: "model".to_string(),
                profile: None,
                protocol: Some("responses".to_string()),
                capabilities: std::collections::BTreeMap::new(),
            }),
            strategy: StrategyIdentity {
                decision_id: "decision-1".to_string(),
                policy_revision: "policy-1".to_string(),
                decision_source: "test".to_string(),
                selected_candidate: candidate,
                selected_pattern: "execute".to_string(),
            },
            timing: OutcomeTiming {
                started_at_ms: 1,
                completed_at_ms: 2,
                duration_ms: 1,
            },
            usage: OutcomeUsage::default(),
            terminal: OutcomeTerminalClass::Succeeded("completed".to_string()),
            quality: OutcomeQuality::Unknown,
            observation: OutcomeObservation {
                source: "test".to_string(),
                observed_at_ms: 2,
                freshness_ms: 0,
            },
            strategy_feedback: Default::default(),
            evidence_refs: Vec::new(),
            evidence_completeness: EvidenceCompleteness::None,
            schema_revision: OUTCOME_SCHEMA_REVISION,
        }
    }

    #[test]
    fn all_execution_candidates_record_without_graph_ref_and_retry_idempotently() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let build = RuntimeBuildIdentity::new("0.9.685", "a".repeat(40), true);
        let service = OutcomeService::with_build_identity(Arc::clone(&store), build.clone());
        for candidate in [
            ExecutionCandidateKind::Direct,
            ExecutionCandidateKind::ParallelTools,
            ExecutionCandidateKind::Team,
        ] {
            let mut outcome = outcome(candidate);
            match candidate {
                ExecutionCandidateKind::Direct => {}
                ExecutionCandidateKind::ParallelTools => {
                    outcome.identity.agent_id = Some("agent-1".to_string());
                }
                ExecutionCandidateKind::Team => {
                    outcome.identity.team_id = Some("team-1".to_string());
                }
            }
            assert!(!service.record_terminal(&outcome).unwrap().duplicate);
            // Producers cannot create a second build truth. Canonicalization
            // happens before the idempotency comparison.
            outcome.runtime.runtime_revision = "producer-stale".to_string();
            outcome.runtime.build = RuntimeBuildIdentity::new("forged", "b".repeat(40), false);
            assert!(service.record_terminal(&outcome).unwrap().duplicate);
        }
        let outcomes = store
            .all_events(100)
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == OUTCOME_EVENT_KIND)
            .map(|event| serde_json::from_value::<ExecutionOutcome>(event.payload).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(outcomes.len(), 3);
        assert!(outcomes.iter().all(|outcome| {
            outcome.runtime.runtime_revision == build.semver && outcome.runtime.build == build
        }));
    }

    #[test]
    fn duplicate_identity_with_different_payload_is_rejected() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = OutcomeService::new(store);
        let original = outcome(ExecutionCandidateKind::Direct);
        service.record_terminal(&original).unwrap();
        let mut conflicting = original;
        conflicting.terminal = OutcomeTerminalClass::Failed("different".to_string());
        assert!(service
            .record_terminal(&conflicting)
            .expect_err("identity conflict must fail")
            .contains("idempotency conflict"));
    }

    #[test]
    fn paired_calibration_import_is_explicit_validated_and_idempotent() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = OutcomeService::new(Arc::clone(&store));
        let mut calibrated = outcome(ExecutionCandidateKind::Direct);
        calibrated.identity.paired_sample_id = Some("pair-1".to_string());
        calibrated.provider.as_mut().unwrap().profile = Some("default".to_string());
        calibrated.quality =
            OutcomeQuality::estimate(8_500, "blind judge", Some("judge-report-1".to_string()));
        calibrated.evidence_refs = vec![harness_contract::reality::EvidenceRef::observed(
            "evaluation_report",
            "report-1",
        )];
        calibrated.evidence_completeness = EvidenceCompleteness::Sufficient;
        calibrated.strategy_feedback.workload = Some(
            harness_contract::strategy::StrategyWorkloadFingerprint::from_understanding(
                &harness_contract::strategy::understand(
                    &harness_contract::strategy::StrategyInput::from_prompt(
                        "bounded calibration task",
                    ),
                ),
                false,
            ),
        );
        calibrated.strategy_feedback.evaluation_environment = "harness_evaluation".to_string();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            serde_json::to_vec(&serde_json::json!({
                "evaluator": "harness-eval",
                "evaluator_version": "1",
                "outcomes": [calibrated],
            }))
            .unwrap(),
        )
        .unwrap();

        let first = service.import_calibration_file(file.path()).unwrap();
        assert_eq!(first.accepted_execution_ids.len(), 1);
        assert!(!first.duplicate);
        let second = service.import_calibration_file(file.path()).unwrap();
        assert!(second.duplicate);
        assert_eq!(
            store
                .all_events(100)
                .unwrap()
                .into_iter()
                .filter(|event| event.kind == OUTCOME_EVENT_KIND)
                .count(),
            1
        );
    }

    #[test]
    fn scoped_feedback_rejects_invalid_environment_and_workload_shape() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = OutcomeService::new(store);
        let mut invalid = outcome(ExecutionCandidateKind::Direct);
        invalid.strategy_feedback.workload = Some(
            harness_contract::strategy::StrategyWorkloadFingerprint::from_understanding(
                &harness_contract::strategy::understand(
                    &harness_contract::strategy::StrategyInput::from_prompt("inspect runtime"),
                ),
                false,
            ),
        );
        invalid.strategy_feedback.evaluation_environment = "unknown".to_string();
        assert!(service
            .record_terminal(&invalid)
            .expect_err("invalid environment")
            .contains("environment"));

        invalid.strategy_feedback.evaluation_environment = "production".to_string();
        invalid
            .strategy_feedback
            .workload
            .as_mut()
            .unwrap()
            .tool_dag_shape = "arbitrary".to_string();
        assert!(service
            .record_terminal(&invalid)
            .expect_err("invalid workload")
            .contains("fingerprint"));
    }
}
