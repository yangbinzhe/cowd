use serde::{Deserialize, Serialize};

use crate::types::{MemoryCategory, MemoryEntry, MemoryLayer, MemorySource};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MemoryAuthorityLevel {
    Historical = 0,
    AgentInferred = 1,
    SessionFact = 2,
    ProjectConfigured = 3,
    UserConfirmed = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryAuthorityAction {
    KeepExisting,
    SupersedeExisting,
    MarkConflict,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryAuthorityDecision {
    pub action: MemoryAuthorityAction,
    pub existing_level: MemoryAuthorityLevel,
    pub incoming_level: MemoryAuthorityLevel,
    pub reason: String,
}

#[must_use]
pub fn authority_level(entry: &MemoryEntry) -> MemoryAuthorityLevel {
    if matches!(entry.source, MemorySource::UserExplicit) || entry.layer == MemoryLayer::L0 {
        return MemoryAuthorityLevel::UserConfirmed;
    }
    if matches!(
        entry.category,
        MemoryCategory::ProjectConvention | MemoryCategory::ProjectKnowledge
    ) || entry.layer == MemoryLayer::L2
    {
        return MemoryAuthorityLevel::ProjectConfigured;
    }
    if entry.session_id.is_some() {
        return MemoryAuthorityLevel::SessionFact;
    }
    if entry.source_agent.is_some() {
        return MemoryAuthorityLevel::AgentInferred;
    }
    MemoryAuthorityLevel::Historical
}

#[must_use]
pub fn authority_decision(
    existing: &MemoryEntry,
    incoming: &MemoryEntry,
) -> MemoryAuthorityDecision {
    let existing_level = authority_level(existing);
    let incoming_level = authority_level(incoming);
    let same_fact = normalized_fact(existing) == normalized_fact(incoming);

    let (action, reason) = if same_fact {
        (
            MemoryAuthorityAction::Duplicate,
            "incoming memory matches existing effective knowledge".to_string(),
        )
    } else if incoming_level > existing_level {
        (
            MemoryAuthorityAction::SupersedeExisting,
            "incoming memory has higher authority".to_string(),
        )
    } else if incoming_level == existing_level
        && same_memory_key(existing) == same_memory_key(incoming)
    {
        (
            MemoryAuthorityAction::MarkConflict,
            "same authority disagrees on the same memory key".to_string(),
        )
    } else {
        (
            MemoryAuthorityAction::KeepExisting,
            "existing memory remains the effective knowledge".to_string(),
        )
    };

    MemoryAuthorityDecision {
        action,
        existing_level,
        incoming_level,
        reason,
    }
}

#[must_use]
pub fn same_memory_key(entry: &MemoryEntry) -> String {
    let mut tags = entry
        .tags
        .iter()
        .map(|tag| normalize(tag))
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>();
    tags.sort();
    let tag_part = tags.into_iter().take(3).collect::<Vec<_>>().join(",");
    format!(
        "{:?}:{:?}:{}:{}",
        entry.layer,
        entry.category,
        normalize(&entry.title),
        tag_part
    )
}

fn normalized_fact(entry: &MemoryEntry) -> String {
    format!("{}:{}", same_memory_key(entry), normalize(&entry.content))
}

fn normalize(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority,
    };
    use chrono::Utc;
    use uuid::Uuid;

    fn entry(source: MemorySource, title: &str, content: &str) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: vec!["runtime".to_string()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: crate::project_scope::MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::Shared,
        }
    }

    #[test]
    fn user_confirmed_memory_supersedes_agent_inference() {
        let mut existing = entry(MemorySource::AutoExtracted, "Rule", "old");
        existing.source_agent = Some("agent".to_string());
        let incoming = entry(MemorySource::UserExplicit, "Rule", "new");

        let decision = authority_decision(&existing, &incoming);

        assert_eq!(decision.action, MemoryAuthorityAction::SupersedeExisting);
        assert!(decision.incoming_level > decision.existing_level);
    }

    #[test]
    fn equal_authority_disagreement_is_conflict() {
        let existing = entry(MemorySource::UserExplicit, "Rule", "old");
        let incoming = entry(MemorySource::UserExplicit, "Rule", "new");

        let decision = authority_decision(&existing, &incoming);

        assert_eq!(decision.action, MemoryAuthorityAction::MarkConflict);
    }
}
