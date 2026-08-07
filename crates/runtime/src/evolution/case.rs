//! Durable aggregation boundary between noisy Signals and governed evolution work.

use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::{EvolutionSignal, EvolutionSignalSeverity};

pub const EVOLUTION_CASE_WINDOW_MS: u64 = 6 * 60 * 60 * 1_000;
pub const MAX_CASE_SIGNAL_REFS: usize = 64;
pub const MAX_CASE_EVIDENCE_REFS: usize = 256;
pub const EVOLUTION_CASE_CATALOG_PAGE_SIZE: usize = 125;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionCaseState {
    Open,
    Ready,
    Diagnosed,
    Proposed,
    Closed,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCaseKey {
    pub workspace_identity: String,
    pub signal_type: String,
    pub affected_subject: String,
    pub workload_fingerprint: String,
    pub config_definition_revision: String,
    pub provider: String,
    pub model: String,
    pub evaluation_environment: String,
    pub window_start_ms: u64,
}

impl EvolutionCaseKey {
    #[must_use]
    pub fn from_signal(signal: &EvolutionSignal) -> Self {
        let scope = &signal.scope;
        let affected_subject = non_empty(&scope.affected_subject).unwrap_or_else(|| {
            signal
                .source
                .agent_id
                .as_deref()
                .or(signal.source.team_id.as_deref())
                .or(signal.source.session_id.as_deref())
                .or(signal.source.run_id.as_deref())
                .unwrap_or(&signal.source.owner)
                .to_string()
        });
        let created_at_ms = u64::try_from(signal.created_at_ms).unwrap_or(u64::MAX);
        Self {
            workspace_identity: non_empty(&scope.workspace_identity)
                .unwrap_or_else(|| "global".to_string()),
            signal_type: signal.signal_type_label().to_string(),
            affected_subject,
            workload_fingerprint: non_empty(&scope.workload_fingerprint)
                .unwrap_or_else(|| "unscoped".to_string()),
            config_definition_revision: non_empty(&scope.config_definition_revision)
                .unwrap_or_else(|| "unknown".to_string()),
            provider: non_empty(&scope.provider).unwrap_or_else(|| "unknown".to_string()),
            model: non_empty(&scope.model).unwrap_or_else(|| "unknown".to_string()),
            evaluation_environment: non_empty(&scope.evaluation_environment)
                .unwrap_or_else(|| "production".to_string()),
            window_start_ms: created_at_ms / EVOLUTION_CASE_WINDOW_MS * EVOLUTION_CASE_WINDOW_MS,
        }
    }

    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("{:x}", Sha256::digest(bytes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCase {
    pub case_id: String,
    pub key: EvolutionCaseKey,
    pub key_sha256: String,
    pub revision: u64,
    pub state: EvolutionCaseState,
    pub recurrence_count: u64,
    pub critical_count: u64,
    pub signal_ids: Vec<String>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub legacy_refs: Vec<String>,
    pub diagnosis_id: Option<String>,
    pub proposal_id: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCaseSummary {
    pub case_id: String,
    pub state: EvolutionCaseState,
    pub signal_type: String,
    pub affected_subject: String,
    pub recurrence_count: u64,
    pub critical_count: u64,
    pub updated_at_ms: u64,
}

impl From<&EvolutionCase> for EvolutionCaseSummary {
    fn from(case: &EvolutionCase) -> Self {
        Self {
            case_id: case.case_id.clone(),
            state: case.state,
            signal_type: case.key.signal_type.clone(),
            affected_subject: case.key.affected_subject.clone(),
            recurrence_count: case.recurrence_count,
            critical_count: case.critical_count,
            updated_at_ms: case.updated_at_ms,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCaseIndex {
    pub revision: u64,
    pub total_cases: u64,
    pub total_signal_observations: u64,
    pub state_counts: BTreeMap<String, u64>,
    pub recent_cases: Vec<EvolutionCaseSummary>,
    /// Creation-ordered mutable tail. Full pages are frozen into their own
    /// streams so pagination never materializes this index's complete history.
    #[serde(default)]
    pub catalog_tail_page: u64,
    #[serde(default)]
    pub catalog_tail_case_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCaseCatalogPage {
    pub page: u64,
    pub case_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCasePage {
    pub items: Vec<EvolutionCase>,
    pub next_cursor: Option<String>,
    pub total: u64,
}

impl EvolutionCaseIndex {
    #[must_use]
    pub fn apply(
        &self,
        previous: Option<&EvolutionCase>,
        next: &EvolutionCase,
    ) -> EvolutionCaseIndex {
        let mut index = self.clone();
        index.revision = index.revision.saturating_add(1);
        if previous.is_none() {
            index.total_cases = index.total_cases.saturating_add(1);
            if index.catalog_tail_case_ids.len() >= EVOLUTION_CASE_CATALOG_PAGE_SIZE {
                index.catalog_tail_page = index.catalog_tail_page.saturating_add(1);
                index.catalog_tail_case_ids.clear();
            }
            index.catalog_tail_case_ids.push(next.case_id.clone());
        }
        index.total_signal_observations = index.total_signal_observations.saturating_add(
            next.recurrence_count
                .saturating_sub(previous.map_or(0, |case| case.recurrence_count)),
        );
        if let Some(previous) = previous {
            decrement_count(&mut index.state_counts, case_state_label(previous.state));
        }
        *index
            .state_counts
            .entry(case_state_label(next.state).to_string())
            .or_default() += 1;
        index
            .recent_cases
            .retain(|summary| summary.case_id != next.case_id);
        index.recent_cases.push(EvolutionCaseSummary::from(next));
        index.recent_cases.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| left.case_id.cmp(&right.case_id))
        });
        index
            .recent_cases
            .truncate(EVOLUTION_CASE_CATALOG_PAGE_SIZE);
        index
    }
}

impl EvolutionCase {
    #[must_use]
    pub fn from_signal(signal: &EvolutionSignal) -> Self {
        let key = EvolutionCaseKey::from_signal(signal);
        let key_sha256 = key.digest();
        let created_at_ms = u64::try_from(signal.created_at_ms).unwrap_or(u64::MAX);
        let mut case = Self {
            case_id: format!("evo-case-{}", &key_sha256[..24]),
            key,
            key_sha256,
            revision: 0,
            state: EvolutionCaseState::Open,
            recurrence_count: 0,
            critical_count: 0,
            signal_ids: Vec::new(),
            evidence_refs: Vec::new(),
            legacy_refs: Vec::new(),
            diagnosis_id: None,
            proposal_id: None,
            created_at_ms,
            updated_at_ms: created_at_ms,
        };
        case.observe(signal);
        case
    }

    pub fn observe(&mut self, signal: &EvolutionSignal) {
        if self
            .signal_ids
            .iter()
            .any(|signal_id| signal_id == &signal.signal_id)
        {
            return;
        }
        self.recurrence_count = self.recurrence_count.saturating_add(1);
        if signal.severity == EvolutionSignalSeverity::Critical {
            self.critical_count = self.critical_count.saturating_add(1);
        }
        self.signal_ids.push(signal.signal_id.clone());
        if self.signal_ids.len() > MAX_CASE_SIGNAL_REFS {
            self.signal_ids.remove(0);
        }
        for evidence in &signal.evidence_refs {
            if !self.evidence_refs.contains(evidence) {
                self.evidence_refs.push(evidence.clone());
            }
        }
        if self.evidence_refs.len() > MAX_CASE_EVIDENCE_REFS {
            self.evidence_refs
                .drain(..self.evidence_refs.len() - MAX_CASE_EVIDENCE_REFS);
        }
        self.updated_at_ms = u64::try_from(signal.created_at_ms).unwrap_or(u64::MAX);
        if self.state == EvolutionCaseState::Open && self.meets_readiness_threshold() {
            self.state = EvolutionCaseState::Ready;
        }
    }

    #[must_use]
    pub fn meets_readiness_threshold(&self) -> bool {
        self.critical_count > 0 || self.recurrence_count >= 3
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            EvolutionCaseState::Proposed | EvolutionCaseState::Closed | EvolutionCaseState::Expired
        )
    }
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[must_use]
pub const fn case_state_label(state: EvolutionCaseState) -> &'static str {
    match state {
        EvolutionCaseState::Open => "open",
        EvolutionCaseState::Ready => "ready",
        EvolutionCaseState::Diagnosed => "diagnosed",
        EvolutionCaseState::Proposed => "proposed",
        EvolutionCaseState::Closed => "closed",
        EvolutionCaseState::Expired => "expired",
    }
}

fn decrement_count(counts: &mut BTreeMap<String, u64>, state: &str) {
    if let Some(count) = counts.get_mut(state) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvolutionSignalInput, EvolutionSignalSource, EvolutionSignalType};

    fn signal(index: usize, severity: EvolutionSignalSeverity) -> EvolutionSignal {
        let mut signal = EvolutionSignal::new(EvolutionSignalInput {
            signal_type: EvolutionSignalType::MemoryNoise,
            source: EvolutionSignalSource {
                owner: "runtime".to_string(),
                session_id: Some(format!("session-{index}")),
                agent_id: None,
                team_id: None,
                run_id: None,
            },
            evidence_refs: vec![EvidenceRef::observed("test", format!("evidence-{index}"))],
            severity,
            summary: "bounded signal".to_string(),
            suggested_action: "inspect".to_string(),
            immediate_task_can_continue: true,
        });
        signal.signal_id = format!("signal-{index}");
        signal.created_at_ms = 1_000;
        signal.scope.workspace_identity = "workspace".to_string();
        signal.scope.affected_subject = "memory".to_string();
        signal
    }

    #[test]
    fn warning_recurrence_and_critical_signal_have_deterministic_readiness() {
        let first = signal(1, EvolutionSignalSeverity::Warning);
        let mut case = EvolutionCase::from_signal(&first);
        assert_eq!(case.state, EvolutionCaseState::Open);
        case.observe(&signal(2, EvolutionSignalSeverity::Warning));
        assert_eq!(case.state, EvolutionCaseState::Open);
        case.observe(&signal(3, EvolutionSignalSeverity::Warning));
        assert_eq!(case.state, EvolutionCaseState::Ready);

        let critical = EvolutionCase::from_signal(&signal(4, EvolutionSignalSeverity::Critical));
        assert_eq!(critical.state, EvolutionCaseState::Ready);
    }

    #[test]
    fn case_index_rolls_creation_catalog_without_growing_the_tail() {
        let mut index = EvolutionCaseIndex::default();
        for item in 0..=EVOLUTION_CASE_CATALOG_PAGE_SIZE {
            let mut case =
                EvolutionCase::from_signal(&signal(item, EvolutionSignalSeverity::Warning));
            case.case_id = format!("case-{item}");
            index = index.apply(None, &case);
        }
        assert_eq!(index.total_cases, 126);
        assert_eq!(index.catalog_tail_page, 1);
        assert_eq!(index.catalog_tail_case_ids, vec!["case-125"]);
        assert_eq!(index.recent_cases.len(), EVOLUTION_CASE_CATALOG_PAGE_SIZE);
    }
}
