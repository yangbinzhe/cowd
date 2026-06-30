use harness_contract::reality::RecallSourceKind;
use serde::{Deserialize, Serialize};

use crate::types::MemoryId;

use super::RecallCandidate;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecallOmission {
    pub id: MemoryId,
    pub title: String,
    pub source: RecallSourceKind,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecallSourceResult {
    pub source: RecallSourceKind,
    pub status: String,
    pub selected_count: usize,
    pub omitted_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degraded_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecallReport {
    pub selected: Vec<RecallCandidate>,
    pub omitted: Vec<RecallOmission>,
    pub sources: Vec<RecallSourceResult>,
    pub truncated: bool,
}

impl Default for RecallReport {
    fn default() -> Self {
        Self {
            selected: Vec::new(),
            omitted: Vec::new(),
            sources: Vec::new(),
            truncated: false,
        }
    }
}

impl RecallReport {
    pub fn from_selected_omitted(
        selected: Vec<RecallCandidate>,
        omitted: Vec<RecallOmission>,
        mut sources: Vec<RecallSourceResult>,
        truncated: bool,
    ) -> Self {
        if sources.is_empty() {
            sources.push(RecallSourceResult {
                source: RecallSourceKind::Memory,
                status: "enabled_and_wired".to_string(),
                selected_count: selected.len(),
                omitted_count: omitted.len(),
                degraded_reason: None,
            });
        }
        Self {
            selected,
            omitted,
            sources,
            truncated,
        }
    }
}
