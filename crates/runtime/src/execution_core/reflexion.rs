use harness_contract::core::ExecutionMode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReflexionTrigger {
    LowNoveltyToolLoop,
    RepeatedFailure,
    VerificationFailed,
    EvidenceInsufficient,
    UserCorrection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReflexionRecord {
    pub record_id: String,
    pub trigger: ReflexionTrigger,
    pub failed_mode: Option<ExecutionMode>,
    pub recommended_mode: ExecutionMode,
    pub reason_code: String,
    pub evidence_refs: Vec<String>,
    pub retry_budget: usize,
    pub growth_candidate: Option<String>,
}

impl ReflexionRecord {
    #[must_use]
    pub fn low_novelty_tool_loop(reason: impl Into<String>) -> Self {
        Self {
            record_id: format!("reflexion-{}", Uuid::new_v4()),
            trigger: ReflexionTrigger::LowNoveltyToolLoop,
            failed_mode: Some(ExecutionMode::ReActLoop),
            recommended_mode: ExecutionMode::ReflexionRetry,
            reason_code: reason.into(),
            evidence_refs: Vec::new(),
            retry_budget: 1,
            growth_candidate: Some(
                "Prefer batch evidence, ReWOO, Tool DAG, or orchestration before repeating the same tool path."
                    .to_string(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reflexion_record_recommends_retry_mode_with_budget() {
        let record = ReflexionRecord::low_novelty_tool_loop("repeated read");
        assert_eq!(record.recommended_mode, ExecutionMode::ReflexionRetry);
        assert_eq!(record.retry_budget, 1);
        assert!(record.growth_candidate.is_some());
    }
}
