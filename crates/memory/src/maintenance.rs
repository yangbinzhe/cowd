use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::MemoryError;
use crate::types::{MemoryEntry, MemoryId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceCandidateKind {
    Conflict,
    Stale,
    Duplicate,
    AuthorityPromotion,
    RelationshipRefresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceCandidateStatus {
    Open,
    Acknowledged,
    Applied,
    Dismissed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceCandidate {
    pub id: String,
    pub kind: MaintenanceCandidateKind,
    pub status: MaintenanceCandidateStatus,
    pub entry_ids: Vec<MemoryId>,
    pub summary: String,
    pub reason: String,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MaintenanceScanConfig {
    pub stale_threshold: f32,
    pub low_confidence_threshold: f32,
    pub authority_confidence_threshold: f32,
    pub max_candidates: usize,
}

impl Default for MaintenanceScanConfig {
    fn default() -> Self {
        Self {
            stale_threshold: 0.85,
            low_confidence_threshold: 0.45,
            authority_confidence_threshold: 0.92,
            max_candidates: 128,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaintenanceCandidateFilter {
    pub status: Option<MaintenanceCandidateStatus>,
    pub kind: Option<MaintenanceCandidateKind>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct MaintenanceQueue {
    candidates: Arc<Mutex<BTreeMap<String, MaintenanceCandidate>>>,
}

impl MaintenanceQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_many(&self, candidates: Vec<MaintenanceCandidate>) -> Result<usize, MemoryError> {
        let mut guard = self
            .candidates
            .lock()
            .map_err(|_| MemoryError::Store("maintenance queue lock poisoned".to_string()))?;
        let mut inserted = 0;
        for candidate in candidates {
            if !guard.contains_key(&candidate.id) {
                inserted += 1;
            }
            guard.insert(candidate.id.clone(), candidate);
        }
        Ok(inserted)
    }

    pub fn list(
        &self,
        filter: MaintenanceCandidateFilter,
    ) -> Result<Vec<MaintenanceCandidate>, MemoryError> {
        let guard = self
            .candidates
            .lock()
            .map_err(|_| MemoryError::Store("maintenance queue lock poisoned".to_string()))?;
        let mut values = guard
            .values()
            .filter(|candidate| {
                filter
                    .status
                    .is_none_or(|status| candidate.status == status)
                    && filter.kind.is_none_or(|kind| candidate.kind == kind)
            })
            .cloned()
            .collect::<Vec<_>>();
        values.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        if let Some(limit) = filter.limit {
            values.truncate(limit);
        }
        Ok(values)
    }

    pub fn transition(
        &self,
        id: &str,
        status: MaintenanceCandidateStatus,
    ) -> Result<Option<MaintenanceCandidate>, MemoryError> {
        let mut guard = self
            .candidates
            .lock()
            .map_err(|_| MemoryError::Store("maintenance queue lock poisoned".to_string()))?;
        let Some(candidate) = guard.get_mut(id) else {
            return Ok(None);
        };
        candidate.status = status;
        candidate.updated_at = Utc::now();
        Ok(Some(candidate.clone()))
    }
}

pub fn scan_maintenance_candidates(
    entries: &[MemoryEntry],
    config: &MaintenanceScanConfig,
) -> Vec<MaintenanceCandidate> {
    let mut candidates = Vec::new();
    add_stale_candidates(entries, config, &mut candidates);
    add_duplicate_candidates(entries, config, &mut candidates);
    add_conflict_candidates(entries, config, &mut candidates);
    add_authority_candidates(entries, config, &mut candidates);
    add_relationship_refresh_candidates(entries, config, &mut candidates);
    candidates.truncate(config.max_candidates);
    candidates
}

fn new_candidate(
    kind: MaintenanceCandidateKind,
    entry_ids: Vec<MemoryId>,
    summary: String,
    reason: String,
    confidence: f32,
) -> MaintenanceCandidate {
    let now = Utc::now();
    MaintenanceCandidate {
        id: Uuid::new_v4().to_string(),
        kind,
        status: MaintenanceCandidateStatus::Open,
        entry_ids,
        summary,
        reason,
        confidence: confidence.clamp(0.0, 1.0),
        created_at: now,
        updated_at: now,
    }
}

fn add_stale_candidates(
    entries: &[MemoryEntry],
    config: &MaintenanceScanConfig,
    candidates: &mut Vec<MaintenanceCandidate>,
) {
    for entry in entries {
        if entry.staleness >= config.stale_threshold {
            candidates.push(new_candidate(
                MaintenanceCandidateKind::Stale,
                vec![entry.id],
                format!("Review stale memory: {}", entry.title),
                "staleness crossed review threshold; memory remains recoverable".to_string(),
                entry.staleness,
            ));
        }
    }
}

fn add_duplicate_candidates(
    entries: &[MemoryEntry],
    _config: &MaintenanceScanConfig,
    candidates: &mut Vec<MaintenanceCandidate>,
) {
    let mut groups: HashMap<String, Vec<&MemoryEntry>> = HashMap::new();
    for entry in entries {
        groups.entry(normalized_key(entry)).or_default().push(entry);
    }
    for group in groups.values().filter(|group| group.len() > 1) {
        candidates.push(new_candidate(
            MaintenanceCandidateKind::Duplicate,
            group.iter().map(|entry| entry.id).collect(),
            format!("Merge duplicate memories: {}", group[0].title),
            "normalized title and content match".to_string(),
            0.95,
        ));
    }
}

fn add_conflict_candidates(
    entries: &[MemoryEntry],
    config: &MaintenanceScanConfig,
    candidates: &mut Vec<MaintenanceCandidate>,
) {
    let mut by_title: HashMap<String, Vec<&MemoryEntry>> = HashMap::new();
    for entry in entries {
        by_title
            .entry(normalize_text(&entry.title))
            .or_default()
            .push(entry);
    }
    for group in by_title.values().filter(|group| group.len() > 1) {
        let mut contents = group
            .iter()
            .map(|entry| normalize_text(&entry.content))
            .collect::<Vec<_>>();
        contents.sort();
        contents.dedup();
        if contents.len() > 1
            && group
                .iter()
                .any(|entry| entry.confidence <= config.low_confidence_threshold)
        {
            candidates.push(new_candidate(
                MaintenanceCandidateKind::Conflict,
                group.iter().map(|entry| entry.id).collect(),
                format!("Resolve conflicting memories: {}", group[0].title),
                "same title has divergent content and at least one weak-confidence fact".to_string(),
                0.82,
            ));
        }
    }
}

fn add_authority_candidates(
    entries: &[MemoryEntry],
    config: &MaintenanceScanConfig,
    candidates: &mut Vec<MaintenanceCandidate>,
) {
    for entry in entries {
        if entry.confidence >= config.authority_confidence_threshold
            && entry.access_count >= 3
            && entry.staleness < 0.2
        {
            candidates.push(new_candidate(
                MaintenanceCandidateKind::AuthorityPromotion,
                vec![entry.id],
                format!("Promote authoritative memory: {}", entry.title),
                "high-confidence frequently used fresh memory".to_string(),
                entry.confidence,
            ));
        }
    }
}

fn add_relationship_refresh_candidates(
    entries: &[MemoryEntry],
    _config: &MaintenanceScanConfig,
    candidates: &mut Vec<MaintenanceCandidate>,
) {
    for entry in entries {
        if entry.relations.is_empty() && entry.access_count >= 2 {
            candidates.push(new_candidate(
                MaintenanceCandidateKind::RelationshipRefresh,
                vec![entry.id],
                format!("Refresh relationships for memory: {}", entry.title),
                "frequently used memory has no explicit relationships".to_string(),
                0.7,
            ));
        }
    }
}

fn normalized_key(entry: &MemoryEntry) -> String {
    format!(
        "{}\n{}",
        normalize_text(&entry.title),
        normalize_text(&entry.content)
    )
}

fn normalize_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::project_scope::MemoryScope;
    use crate::types::{
        AgentVisibility, MemoryCategory, MemoryLayer, MemorySource, Priority,
    };

    fn entry(title: &str, content: &str) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L2,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::UserExplicit,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: Vec::new(),
            relations: Vec::new(),
            confidence: 0.8,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Session("test-session".to_string()),
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::Private,
        }
    }

    #[test]
    fn scan_creates_stale_candidate_without_deleting_memory() {
        let mut stale = entry("Old decision", "Use provider A");
        stale.staleness = 0.91;

        let candidates =
            scan_maintenance_candidates(&[stale.clone()], &MaintenanceScanConfig::default());

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].kind, MaintenanceCandidateKind::Stale);
        assert_eq!(candidates[0].entry_ids, vec![stale.id]);
        assert!(candidates[0].reason.contains("remains recoverable"));
    }

    #[test]
    fn scan_detects_duplicates_and_conflicts() {
        let a = entry("Session store", "SQLite is authoritative");
        let duplicate = entry(" Session   store ", "SQLite is authoritative");
        let mut weak_conflict = entry("Session store", "JSONL is authoritative");
        weak_conflict.confidence = 0.2;

        let candidates =
            scan_maintenance_candidates(&[a, duplicate, weak_conflict], &MaintenanceScanConfig::default());

        assert!(candidates
            .iter()
            .any(|candidate| candidate.kind == MaintenanceCandidateKind::Duplicate));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.kind == MaintenanceCandidateKind::Conflict));
    }

    #[test]
    fn queue_lists_and_transitions_candidates() {
        let queue = MaintenanceQueue::new();
        let candidate = new_candidate(
            MaintenanceCandidateKind::AuthorityPromotion,
            vec![Uuid::new_v4()],
            "Promote memory".to_string(),
            "trusted and frequently used".to_string(),
            0.93,
        );
        let id = candidate.id.clone();

        assert_eq!(queue.upsert_many(vec![candidate]).unwrap(), 1);
        assert_eq!(
            queue
                .list(MaintenanceCandidateFilter {
                    status: Some(MaintenanceCandidateStatus::Open),
                    ..MaintenanceCandidateFilter::default()
                })
                .unwrap()
                .len(),
            1
        );

        let updated = queue
            .transition(&id, MaintenanceCandidateStatus::Acknowledged)
            .unwrap()
            .expect("candidate should exist");

        assert_eq!(updated.status, MaintenanceCandidateStatus::Acknowledged);
        assert!(queue
            .list(MaintenanceCandidateFilter {
                status: Some(MaintenanceCandidateStatus::Open),
                ..MaintenanceCandidateFilter::default()
            })
            .unwrap()
            .is_empty());
    }
}
