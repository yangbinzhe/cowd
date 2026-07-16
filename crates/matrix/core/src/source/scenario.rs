//! Immutable Matrix scenario contracts.
//!
//! A scenario never changes an observed Matrix source snapshot. It records a
//! bounded set of assumptions and produces a result with an explicit
//! `simulated` boundary that must pass a separate promotion path before it can
//! affect durable facts or memory.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::source::MatrixSourceSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixSnapshotRef {
    pub snapshot_id: String,
    pub snapshot_ref: String,
    pub content_digest: String,
}

impl MatrixSnapshotRef {
    #[must_use]
    pub fn from_source_snapshot(snapshot: &MatrixSourceSnapshot) -> Self {
        let serialized = canonical_json_bytes(snapshot);
        Self {
            snapshot_id: snapshot.snapshot_id.clone(),
            snapshot_ref: snapshot.reference(),
            content_digest: format!("{:x}", Sha256::digest(serialized)),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.snapshot_id.trim().is_empty() || self.snapshot_ref.trim().is_empty() {
            return Err("matrix snapshot reference requires an id and reference".to_string());
        }
        if self.snapshot_ref != format!("matrix:source_snapshot:{}", self.snapshot_id) {
            return Err("matrix snapshot reference does not match its snapshot id".to_string());
        }
        validate_digest("snapshot content digest", &self.content_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixScenarioOutputContract {
    pub required_outputs: Vec<String>,
    pub evidence_required: bool,
}

impl MatrixScenarioOutputContract {
    pub fn validate(&self) -> Result<(), String> {
        if self.required_outputs.is_empty() {
            return Err("scenario output contract requires at least one output".to_string());
        }
        if self
            .required_outputs
            .iter()
            .any(|output| output.trim().is_empty())
        {
            return Err("scenario output names cannot be blank".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixScenarioSpec {
    pub scenario_id: String,
    pub base_snapshot: MatrixSnapshotRef,
    pub assumptions: Value,
    pub transform_ref: String,
    pub output_contract: MatrixScenarioOutputContract,
    pub created_at: DateTime<Utc>,
}

impl MatrixScenarioSpec {
    #[must_use]
    pub fn new(
        base_snapshot: MatrixSnapshotRef,
        assumptions: Value,
        transform_ref: impl Into<String>,
        output_contract: MatrixScenarioOutputContract,
    ) -> Self {
        Self {
            scenario_id: format!("scenario-{}", Uuid::new_v4()),
            base_snapshot,
            assumptions,
            transform_ref: transform_ref.into(),
            output_contract,
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn reference(&self) -> String {
        format!("matrix:scenario:{}", self.scenario_id)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.scenario_id.trim().is_empty() || self.transform_ref.trim().is_empty() {
            return Err("scenario id and transform reference are required".to_string());
        }
        self.base_snapshot.validate()?;
        self.output_contract.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MatrixScenarioRunStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixScenarioRun {
    pub run_id: String,
    pub scenario_id: String,
    pub base_snapshot: MatrixSnapshotRef,
    pub parameters: Value,
    pub input_digest: String,
    pub status: MatrixScenarioRunStatus,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

impl MatrixScenarioRun {
    #[must_use]
    pub fn start(spec: &MatrixScenarioSpec, parameters: Value) -> Self {
        let input = serde_json::json!({
            "scenario": spec,
            "parameters": parameters,
        });
        let input_digest = format!("{:x}", Sha256::digest(canonical_json_bytes(&input)));
        Self {
            run_id: format!("scenario-run-{}", Uuid::new_v4()),
            scenario_id: spec.scenario_id.clone(),
            base_snapshot: spec.base_snapshot.clone(),
            parameters,
            input_digest,
            status: MatrixScenarioRunStatus::Running,
            started_at: Utc::now(),
            completed_at: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.run_id.trim().is_empty() || self.scenario_id.trim().is_empty() {
            return Err("scenario run requires ids".to_string());
        }
        self.base_snapshot.validate()?;
        validate_digest("scenario input digest", &self.input_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixScenarioResult {
    pub result_id: String,
    pub run_id: String,
    pub scenario_id: String,
    /// Scenario results are always simulated. This is intentionally a stable
    /// literal instead of a mutable caller-supplied reality boundary.
    pub boundary: String,
    pub outputs: Value,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub output_digest: String,
    pub completed_at: DateTime<Utc>,
}

impl MatrixScenarioResult {
    #[must_use]
    pub fn simulated(run: &MatrixScenarioRun, outputs: Value, evidence_refs: Vec<String>) -> Self {
        let output_digest = format!("{:x}", Sha256::digest(canonical_json_bytes(&outputs)));
        Self {
            result_id: format!("scenario-result-{}", Uuid::new_v4()),
            run_id: run.run_id.clone(),
            scenario_id: run.scenario_id.clone(),
            boundary: "simulated".to_string(),
            outputs,
            evidence_refs,
            output_digest,
            completed_at: Utc::now(),
        }
    }

    pub fn validate_for_run(&self, run: &MatrixScenarioRun) -> Result<(), String> {
        if self.result_id.trim().is_empty()
            || self.run_id != run.run_id
            || self.scenario_id != run.scenario_id
        {
            return Err("scenario result does not belong to the scenario run".to_string());
        }
        if self.boundary != "simulated" {
            return Err("scenario results must retain the simulated boundary".to_string());
        }
        validate_digest("scenario output digest", &self.output_digest)
    }
}

fn validate_digest(label: &str, value: &str) -> Result<(), String> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(format!("{label} must be a lowercase SHA-256 digest"))
    }
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_else(|error| error.to_string().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MatrixSourceKind;

    #[test]
    fn scenario_result_is_always_simulated_and_bound_to_its_snapshot() {
        let snapshot = MatrixSourceSnapshot::new("fixture", MatrixSourceKind::Manual, "v1");
        let spec = MatrixScenarioSpec::new(
            MatrixSnapshotRef::from_source_snapshot(&snapshot),
            serde_json::json!({"demand_delta": 0.2}),
            "runtime/scenario/linear-demand@1",
            MatrixScenarioOutputContract {
                required_outputs: vec!["shortage_risk".to_string()],
                evidence_required: true,
            },
        );
        spec.validate().unwrap();
        let run = MatrixScenarioRun::start(&spec, serde_json::json!({"region": "east"}));
        run.validate().unwrap();
        let result = MatrixScenarioResult::simulated(
            &run,
            serde_json::json!({"shortage_risk": "high"}),
            vec![snapshot.reference()],
        );
        result.validate_for_run(&run).unwrap();
        assert_eq!(result.boundary, "simulated");
    }
}
