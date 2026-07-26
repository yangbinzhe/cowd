use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::AppContractError;

pub const APPLICATION_EXECUTION_OUTCOME_VERSION: u16 = 1;

const MAX_ID_BYTES: usize = 256;
const MAX_TITLE_BYTES: usize = 512;
const MAX_SUMMARY_BYTES: usize = 16 * 1024;
const MAX_REFS: usize = 128;
const MAX_TAGS: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationExecutionKind {
    Tool,
    Agent,
    Task,
    StructuredIngest,
    StructuredFact,
    StructuredEvidence,
    ApplicationCompute,
    ApplicationAction,
    SkillRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationExecutionStatus {
    Planned,
    Running,
    Succeeded,
    Failed,
    Blocked,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionRefV1 {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionOutcomeV1 {
    pub contract_version: u16,
    pub outcome_id: String,
    pub kind: ApplicationExecutionKind,
    pub status: ApplicationExecutionStatus,
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub refs: Vec<ApplicationExecutionRefV1>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub metrics: Vec<String>,
    #[serde(default)]
    pub payload: Value,
    pub occurred_at_ms: u64,
}

impl ApplicationExecutionOutcomeV1 {
    pub fn validate(&self) -> Result<(), AppContractError> {
        if self.contract_version != APPLICATION_EXECUTION_OUTCOME_VERSION {
            return Err(AppContractError::InvalidApplicationExecutionOutcome(
                format!(
                    "unsupported contract_version {}; expected {}",
                    self.contract_version, APPLICATION_EXECUTION_OUTCOME_VERSION
                ),
            ));
        }
        validate_required("outcome_id", &self.outcome_id, MAX_ID_BYTES)?;
        validate_required("title", &self.title, MAX_TITLE_BYTES)?;
        validate_required("summary", &self.summary, MAX_SUMMARY_BYTES)?;
        if self
            .domain
            .as_deref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_ID_BYTES)
        {
            return Err(AppContractError::InvalidApplicationExecutionOutcome(
                "domain is empty or exceeds 256 bytes".to_string(),
            ));
        }
        if self.refs.len() > MAX_REFS
            || self.evidence_refs.len() > MAX_TAGS
            || self.metrics.len() > MAX_TAGS
        {
            return Err(AppContractError::InvalidApplicationExecutionOutcome(
                "refs, evidence_refs, or metrics exceed the bounded collection size".to_string(),
            ));
        }
        for reference in &self.refs {
            validate_required("ref.type", &reference.ref_type, MAX_ID_BYTES)?;
            validate_required("ref.id", &reference.id, MAX_ID_BYTES)?;
            if reference
                .label
                .as_deref()
                .is_some_and(|value| value.len() > MAX_TITLE_BYTES)
            {
                return Err(AppContractError::InvalidApplicationExecutionOutcome(
                    "ref.label exceeds 512 bytes".to_string(),
                ));
            }
        }
        for value in self.evidence_refs.iter().chain(self.metrics.iter()) {
            validate_required("reference", value, MAX_ID_BYTES)?;
        }
        let payload_bytes = serde_json::to_vec(&self.payload).map_err(|error| {
            AppContractError::InvalidApplicationExecutionOutcome(format!(
                "payload cannot be serialized: {error}"
            ))
        })?;
        if payload_bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(AppContractError::InvalidApplicationExecutionOutcome(
                "payload exceeds 1 MiB".to_string(),
            ));
        }
        Ok(())
    }
}

fn validate_required(field: &str, value: &str, max_bytes: usize) -> Result<(), AppContractError> {
    if value.trim().is_empty() || value.len() > max_bytes {
        return Err(AppContractError::InvalidApplicationExecutionOutcome(
            format!("{field} is empty or exceeds {max_bytes} bytes"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome() -> ApplicationExecutionOutcomeV1 {
        ApplicationExecutionOutcomeV1 {
            contract_version: APPLICATION_EXECUTION_OUTCOME_VERSION,
            outcome_id: "outcome-1".to_string(),
            kind: ApplicationExecutionKind::ApplicationAction,
            status: ApplicationExecutionStatus::Succeeded,
            title: "Action completed".to_string(),
            summary: "The requested application action completed.".to_string(),
            domain: Some("manufacturing".to_string()),
            refs: vec![ApplicationExecutionRefV1 {
                ref_type: "action".to_string(),
                id: "action-1".to_string(),
                label: None,
            }],
            evidence_refs: vec!["receipt://action-1".to_string()],
            metrics: Vec::new(),
            payload: serde_json::json!({"revision": 3}),
            occurred_at_ms: 42,
        }
    }

    #[test]
    fn execution_outcome_contract_is_versioned_and_bounded() {
        outcome().validate().unwrap();

        let mut invalid = outcome();
        invalid.contract_version = 0;
        assert!(invalid.validate().is_err());

        let mut oversized = outcome();
        oversized.payload = serde_json::json!({"value": "x".repeat(MAX_PAYLOAD_BYTES)});
        assert!(oversized.validate().is_err());
    }
}
