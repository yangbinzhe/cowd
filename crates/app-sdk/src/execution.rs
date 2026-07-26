use serde::{Deserialize, Serialize};

use crate::AppContractError;

pub const APPLICATION_EXECUTION_OUTCOME_VERSION: u16 = 1;
pub const APPEND_APPLICATION_EXECUTION_OUTCOME_INTENT_V1: &str =
    "cowd.work_context.append_application_execution_outcome.v1";

const MAX_ID_BYTES: usize = 256;
const MAX_TITLE_BYTES: usize = 512;
const MAX_SUMMARY_BYTES: usize = 16 * 1024;
const MAX_REFS: usize = 128;
const MAX_COUNTERS: usize = 128;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionCounterV1 {
    pub name: String,
    pub value: i64,
}

/// Stable APP-to-host summary contract.
///
/// APP-owned facts and artifacts remain in their authoritative stores. This
/// contract carries only bounded references and counters into the Session
/// timeline, so replay never creates a second mutable copy of APP state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub metric_refs: Vec<String>,
    #[serde(default)]
    pub counters: Vec<ApplicationExecutionCounterV1>,
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
            || self.evidence_refs.len() > MAX_REFS
            || self.metric_refs.len() > MAX_REFS
            || self.counters.len() > MAX_COUNTERS
        {
            return Err(AppContractError::InvalidApplicationExecutionOutcome(
                "refs, evidence_refs, metric_refs, or counters exceed the bounded collection size"
                    .to_string(),
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
        for value in self.evidence_refs.iter().chain(self.metric_refs.iter()) {
            validate_required("reference", value, MAX_ID_BYTES)?;
        }
        for counter in &self.counters {
            validate_required("counter.name", &counter.name, MAX_ID_BYTES)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionOutcomeIntentV1 {
    pub session_id: String,
    pub outcome: ApplicationExecutionOutcomeV1,
}

impl ApplicationExecutionOutcomeIntentV1 {
    pub fn validate(&self) -> Result<(), AppContractError> {
        validate_required("session_id", &self.session_id, MAX_ID_BYTES)?;
        self.outcome.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionOutcomeReceiptV1 {
    pub sequence: u64,
    pub replayed: bool,
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
            metric_refs: vec!["metric://cycle-time".to_string()],
            counters: vec![ApplicationExecutionCounterV1 {
                name: "affected_rows".to_string(),
                value: 3,
            }],
            occurred_at_ms: 42,
        }
    }

    #[test]
    fn execution_outcome_contract_is_versioned_bounded_and_payload_free() {
        outcome().validate().unwrap();

        let mut invalid = outcome();
        invalid.contract_version = 0;
        assert!(invalid.validate().is_err());

        let mut oversized = outcome();
        oversized.summary = "x".repeat(MAX_SUMMARY_BYTES + 1);
        assert!(oversized.validate().is_err());

        let serialized = serde_json::to_value(outcome()).unwrap();
        assert!(serialized.get("payload").is_none());
    }

    #[test]
    fn neutral_intent_and_receipt_reject_unknown_fields() {
        ApplicationExecutionOutcomeIntentV1 {
            session_id: "session-1".to_string(),
            outcome: outcome(),
        }
        .validate()
        .unwrap();

        let invalid = serde_json::json!({
            "sequence": 7,
            "replayed": false,
            "payload": {}
        });
        assert!(serde_json::from_value::<ApplicationExecutionOutcomeReceiptV1>(invalid).is_err());
    }
}
