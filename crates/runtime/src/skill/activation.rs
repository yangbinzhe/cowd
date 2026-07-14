//! Session-domain event support for skill activation decisions.

use harness_contract::skill::SkillInvocationEvidence;
use memory::{SessionDomainEvent, SessionDomainRef, SessionDomainScope};
use serde::{Deserialize, Serialize};

use super::CowdSkillStructuredDependency;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSkillCandidateSource {
    Profile,
    CapabilityRefFallback,
}

impl Default for RuntimeSkillCandidateSource {
    fn default() -> Self {
        Self::Profile
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSkillCandidate {
    pub name: String,
    pub score: u32,
    pub reasons: Vec<String>,
    pub path: Option<String>,
    #[serde(default)]
    pub source: RuntimeSkillCandidateSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillActivationRecord {
    pub session_id: String,
    pub turn_index: usize,
    pub query: String,
    pub selected: Option<String>,
    pub candidates: Vec<RuntimeSkillCandidate>,
    pub invocation_evidence: Option<SkillInvocationEvidence>,
    pub structured_dependencies: Vec<CowdSkillStructuredDependency>,
}

impl SkillActivationRecord {
    #[must_use]
    pub fn new(
        session_id: impl Into<String>,
        turn_index: usize,
        query: impl Into<String>,
        candidates: Vec<RuntimeSkillCandidate>,
    ) -> Self {
        let selected = candidates
            .iter()
            .find(|candidate| candidate.source == RuntimeSkillCandidateSource::Profile)
            .map(|candidate| candidate.name.clone());
        Self {
            session_id: session_id.into(),
            turn_index,
            query: query.into(),
            selected,
            candidates,
            invocation_evidence: None,
            structured_dependencies: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_invocation_evidence(mut self, evidence: Option<SkillInvocationEvidence>) -> Self {
        self.invocation_evidence = evidence;
        self
    }

    #[must_use]
    pub fn with_structured_dependencies(
        mut self,
        dependencies: Vec<CowdSkillStructuredDependency>,
    ) -> Self {
        self.structured_dependencies = dependencies;
        self
    }

    #[must_use]
    pub fn to_session_domain_event(&self, sequence: usize) -> SessionDomainEvent {
        let payload = serde_json::json!({
            "source": "conversation_runtime.skill_activation",
            "turn_index": self.turn_index,
            "query": self.query,
            "selected": self.selected,
            "candidates": self.candidates,
            "invocation_evidence": self.invocation_evidence,
            "structured_dependencies": self.structured_dependencies,
        });
        let mut event = SessionDomainEvent::new(
            self.session_id.clone(),
            sequence,
            SessionDomainScope::Context,
            "skill_candidates",
            payload,
            now_ms(),
        );
        if let Some(selected) = &self.selected {
            event.refs.push(SessionDomainRef {
                ref_type: "skill".to_string(),
                id: selected.clone(),
                label: Some("selected".to_string()),
            });
        }
        if let Some(evidence) = &self.invocation_evidence {
            event.refs.push(SessionDomainRef {
                ref_type: "skill_invocation".to_string(),
                id: evidence.skill_id.clone(),
                label: Some(evidence.outcome.clone()),
            });
        }
        for dependency in &self.structured_dependencies {
            event.refs.push(SessionDomainRef {
                ref_type: "skill_dependency".to_string(),
                id: format!("{}:{}", dependency.skill_id, dependency.domain),
                label: Some(dependency.quality_gate.clone()),
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
    fn activation_record_projects_to_session_domain_event() {
        let record = SkillActivationRecord::new(
            "session-1",
            2,
            "prepare release",
            vec![RuntimeSkillCandidate {
                name: "release".to_string(),
                score: 12,
                reasons: vec!["tags:1".to_string()],
                path: Some("/tmp/release/SKILL.md".to_string()),
                source: RuntimeSkillCandidateSource::Profile,
            }],
        );

        let event = record.to_session_domain_event(7);

        assert_eq!(event.session_id, "session-1");
        assert_eq!(event.sequence, 7);
        assert_eq!(event.scope, SessionDomainScope::Context);
        assert_eq!(event.kind, "skill_candidates");
        assert_eq!(event.payload["selected"], "release");
        assert!(event.payload.get("invocation_evidence").is_some());
        assert!(event.payload["structured_dependencies"].is_array());
        assert_eq!(event.refs[0].ref_type, "skill");
        assert_eq!(event.refs[0].id, "release");
    }
}
