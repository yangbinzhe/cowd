//! Runtime event support for skill activation decisions.

use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSkillCandidate {
    pub name: String,
    pub score: u32,
    pub reasons: Vec<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillActivationRecord {
    pub session_id: String,
    pub turn_index: usize,
    pub query: String,
    pub selected: Option<String>,
    pub candidates: Vec<RuntimeSkillCandidate>,
}

impl SkillActivationRecord {
    #[must_use]
    pub fn new(
        session_id: impl Into<String>,
        turn_index: usize,
        query: impl Into<String>,
        candidates: Vec<RuntimeSkillCandidate>,
    ) -> Self {
        let selected = candidates.first().map(|candidate| candidate.name.clone());
        Self {
            session_id: session_id.into(),
            turn_index,
            query: query.into(),
            selected,
            candidates,
        }
    }

    #[must_use]
    pub fn to_runtime_event(&self, sequence: usize) -> RuntimeEvent {
        let payload = serde_json::json!({
            "turn_index": self.turn_index,
            "query": self.query,
            "selected": self.selected,
            "candidates": self.candidates,
        });
        let mut event = RuntimeEvent::new(
            self.session_id.clone(),
            sequence,
            RuntimeEventScope::Context,
            "skill_candidates",
            payload,
            now_ms(),
        );
        if let Some(selected) = &self.selected {
            event.refs.push(RuntimeRef {
                ref_type: "skill".to_string(),
                id: selected.clone(),
                label: Some("selected".to_string()),
            });
        }
        event
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_record_projects_to_runtime_event() {
        let record = SkillActivationRecord::new(
            "session-1",
            2,
            "prepare release",
            vec![RuntimeSkillCandidate {
                name: "release".to_string(),
                score: 12,
                reasons: vec!["tags:1".to_string()],
                path: Some("/tmp/release/SKILL.md".to_string()),
            }],
        );

        let event = record.to_runtime_event(7);

        assert_eq!(event.session_id, "session-1");
        assert_eq!(event.sequence, 7);
        assert_eq!(event.scope, RuntimeEventScope::Context);
        assert_eq!(event.kind, "skill_candidates");
        assert_eq!(event.payload["selected"], "release");
        assert_eq!(event.refs[0].ref_type, "skill");
        assert_eq!(event.refs[0].id, "release");
    }
}
