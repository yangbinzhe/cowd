//! Mission evidence bus for team, agent, approval, tool, and recovery outputs.

use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::{record_runtime_event, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionEvidenceRef {
    pub evidence_id: String,
    pub mission_id: Option<String>,
    pub session_id: String,
    pub team_id: Option<String>,
    pub agent_id: Option<String>,
    pub kind: String,
    pub summary: String,
    pub source_ref: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Default)]
pub struct MissionEvidenceBus {
    evidence: Mutex<Vec<MissionEvidenceRef>>,
}

impl MissionEvidenceBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, evidence: MissionEvidenceRef) -> MissionEvidenceRef {
        let evidence = MissionEvidenceRef {
            evidence_id: if evidence.evidence_id.trim().is_empty() {
                format!("mission-evidence-{}", uuid::Uuid::new_v4())
            } else {
                evidence.evidence_id
            },
            created_at_ms: if evidence.created_at_ms == 0 {
                now_ms()
            } else {
                evidence.created_at_ms
            },
            ..evidence
        };
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(evidence.clone());
        record_event(&evidence);
        evidence
    }

    #[must_use]
    pub fn list_for_session(&self, session_id: &str) -> Vec<MissionEvidenceRef> {
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|item| item.session_id == session_id)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn list_for_team(&self, team_id: &str) -> Vec<MissionEvidenceRef> {
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|item| item.team_id.as_deref() == Some(team_id))
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn list_all(&self) -> Vec<MissionEvidenceRef> {
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn projection(&self) -> serde_json::Value {
        let mut evidence = self.list_all();
        evidence.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        serde_json::json!({
            "kind": "runtime.mission_evidence",
            "count": evidence.len(),
            "latest": evidence.into_iter().take(100).collect::<Vec<_>>(),
        })
    }
}

pub fn global_mission_evidence_bus() -> &'static MissionEvidenceBus {
    static BUS: OnceLock<MissionEvidenceBus> = OnceLock::new();
    BUS.get_or_init(MissionEvidenceBus::new)
}

fn record_event(evidence: &MissionEvidenceRef) {
    let _ = record_runtime_event(RuntimeEventInput {
        stream_id: evidence
            .team_id
            .as_ref()
            .map(|team_id| format!("team:{team_id}"))
            .unwrap_or_else(|| format!("session:{}", evidence.session_id)),
        scope: RuntimeEventScope::Tool,
        kind: "mission_evidence.recorded".to_string(),
        status: Some("recorded".to_string()),
        actor: Some("mission_evidence_bus".to_string()),
        refs: vec![RuntimeEventRef {
            kind: "evidence".to_string(),
            id: evidence.evidence_id.clone(),
        }],
        payload: serde_json::json!(evidence),
    });
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
