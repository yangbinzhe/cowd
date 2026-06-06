use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
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

impl MaintenanceCandidateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conflict => "conflict",
            Self::Stale => "stale",
            Self::Duplicate => "duplicate",
            Self::AuthorityPromotion => "authority_promotion",
            Self::RelationshipRefresh => "relationship_refresh",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "conflict" => Some(Self::Conflict),
            "stale" => Some(Self::Stale),
            "duplicate" => Some(Self::Duplicate),
            "authority_promotion" => Some(Self::AuthorityPromotion),
            "relationship_refresh" => Some(Self::RelationshipRefresh),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceCandidateStatus {
    Open,
    Acknowledged,
    Applied,
    Dismissed,
}

impl MaintenanceCandidateStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Acknowledged => "acknowledged",
            Self::Applied => "applied",
            Self::Dismissed => "dismissed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "acknowledged" => Some(Self::Acknowledged),
            "applied" => Some(Self::Applied),
            "dismissed" => Some(Self::Dismissed),
            _ => None,
        }
    }
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
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub source_ref: Option<String>,
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
    pub source: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct MaintenanceQueue {
    candidates: Arc<Mutex<BTreeMap<String, MaintenanceCandidate>>>,
    sqlite_path: Option<Arc<PathBuf>>,
}

impl MaintenanceQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self, MemoryError> {
        let path = path.as_ref().to_path_buf();
        let queue = Self {
            candidates: Arc::new(Mutex::new(BTreeMap::new())),
            sqlite_path: Some(Arc::new(path)),
        };
        queue.init_durable_schema()?;
        Ok(queue)
    }

    pub fn upsert_many(&self, candidates: Vec<MaintenanceCandidate>) -> Result<usize, MemoryError> {
        if self.sqlite_path.is_some() {
            let inserted = self.upsert_many_durable(&candidates)?;
            let mut guard = self
                .candidates
                .lock()
                .map_err(|_| MemoryError::Store("maintenance queue lock poisoned".to_string()))?;
            for candidate in candidates {
                guard.insert(candidate.id.clone(), candidate);
            }
            return Ok(inserted);
        }
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
        if self.sqlite_path.is_some() {
            return self.list_durable(filter);
        }
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
                    && filter
                        .source
                        .as_ref()
                        .is_none_or(|source| candidate.source.as_ref() == Some(source))
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
        if self.sqlite_path.is_some() {
            let updated = self.transition_durable(id, status)?;
            if let Some(candidate) = &updated {
                let mut guard = self.candidates.lock().map_err(|_| {
                    MemoryError::Store("maintenance queue lock poisoned".to_string())
                })?;
                guard.insert(candidate.id.clone(), candidate.clone());
            }
            return Ok(updated);
        }
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

    fn conn(&self) -> Result<Connection, MemoryError> {
        let Some(path) = &self.sqlite_path else {
            return Err(MemoryError::Store(
                "durable maintenance queue not configured".to_string(),
            ));
        };
        Connection::open(path.as_ref()).map_err(sqlite_err)
    }

    fn init_durable_schema(&self) -> Result<(), MemoryError> {
        let conn = self.conn()?;
        conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS memory_maintenance_candidates (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_ids_json TEXT NOT NULL,
                summary TEXT NOT NULL,
                reason TEXT NOT NULL,
                confidence REAL NOT NULL,
                source TEXT,
                source_ref TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_memory_maintenance_status
                ON memory_maintenance_candidates(status, updated_at);
            CREATE INDEX IF NOT EXISTS idx_memory_maintenance_kind
                ON memory_maintenance_candidates(kind, updated_at);
            CREATE INDEX IF NOT EXISTS idx_memory_maintenance_source
                ON memory_maintenance_candidates(source, updated_at);
            ",
        )
        .map_err(sqlite_err)
    }

    fn upsert_many_durable(
        &self,
        candidates: &[MaintenanceCandidate],
    ) -> Result<usize, MemoryError> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sqlite_err)?;
        let mut inserted = 0usize;
        for candidate in candidates {
            let existed = tx
                .query_row(
                    "SELECT 1 FROM memory_maintenance_candidates WHERE id = ?1",
                    params![candidate.id],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sqlite_err)?
                .is_some();
            if !existed {
                inserted += 1;
            }
            let entry_ids_json = serde_json::to_string(&candidate.entry_ids)
                .map_err(MemoryError::Serialisation)?;
            tx.execute(
                r"INSERT INTO memory_maintenance_candidates
                  (id, kind, status, entry_ids_json, summary, reason, confidence,
                   source, source_ref, created_at, updated_at)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                  ON CONFLICT(id) DO UPDATE SET
                    kind = excluded.kind,
                    status = excluded.status,
                    entry_ids_json = excluded.entry_ids_json,
                    summary = excluded.summary,
                    reason = excluded.reason,
                    confidence = excluded.confidence,
                    source = excluded.source,
                    source_ref = excluded.source_ref,
                    updated_at = excluded.updated_at",
                params![
                    candidate.id,
                    candidate.kind.as_str(),
                    candidate.status.as_str(),
                    entry_ids_json,
                    candidate.summary,
                    candidate.reason,
                    candidate.confidence,
                    candidate.source,
                    candidate.source_ref,
                    candidate.created_at.to_rfc3339(),
                    candidate.updated_at.to_rfc3339(),
                ],
            )
            .map_err(sqlite_err)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(inserted)
    }

    fn list_durable(
        &self,
        filter: MaintenanceCandidateFilter,
    ) -> Result<Vec<MaintenanceCandidate>, MemoryError> {
        let conn = self.conn()?;
        let limit = filter.limit.unwrap_or(128).min(500) as i64;
        let mut candidates = Vec::new();
        let mut stmt = conn
            .prepare(
                r"SELECT id, kind, status, entry_ids_json, summary, reason, confidence,
                         source, source_ref, created_at, updated_at
                    FROM memory_maintenance_candidates
                   ORDER BY datetime(created_at) DESC
                   LIMIT ?1",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(params![limit], row_to_candidate)
            .map_err(sqlite_err)?;
        for row in rows {
            let candidate = row.map_err(sqlite_err)?;
            if filter
                .status
                .is_some_and(|status| candidate.status != status)
                || filter.kind.is_some_and(|kind| candidate.kind != kind)
                || filter
                    .source
                    .as_ref()
                    .is_some_and(|source| candidate.source.as_ref() != Some(source))
            {
                continue;
            }
            candidates.push(candidate);
        }
        Ok(candidates)
    }

    fn transition_durable(
        &self,
        id: &str,
        status: MaintenanceCandidateStatus,
    ) -> Result<Option<MaintenanceCandidate>, MemoryError> {
        let conn = self.conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE memory_maintenance_candidates SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status.as_str(), now, id],
        )
        .map_err(sqlite_err)?;
        let mut stmt = conn
            .prepare(
                r"SELECT id, kind, status, entry_ids_json, summary, reason, confidence,
                         source, source_ref, created_at, updated_at
                    FROM memory_maintenance_candidates
                   WHERE id = ?1",
            )
            .map_err(sqlite_err)?;
        stmt.query_row(params![id], row_to_candidate)
            .optional()
            .map_err(sqlite_err)
    }
}

fn row_to_candidate(row: &rusqlite::Row<'_>) -> rusqlite::Result<MaintenanceCandidate> {
    let kind_raw: String = row.get(1)?;
    let status_raw: String = row.get(2)?;
    let entry_ids_json: String = row.get(3)?;
    let created_at_raw: String = row.get(9)?;
    let updated_at_raw: String = row.get(10)?;
    Ok(MaintenanceCandidate {
        id: row.get(0)?,
        kind: MaintenanceCandidateKind::parse(&kind_raw)
            .unwrap_or(MaintenanceCandidateKind::RelationshipRefresh),
        status: MaintenanceCandidateStatus::parse(&status_raw)
            .unwrap_or(MaintenanceCandidateStatus::Open),
        entry_ids: serde_json::from_str(&entry_ids_json).unwrap_or_default(),
        summary: row.get(4)?,
        reason: row.get(5)?,
        confidence: row.get::<_, f32>(6)?.clamp(0.0, 1.0),
        source: row.get(7)?,
        source_ref: row.get(8)?,
        created_at: DateTime::parse_from_rfc3339(&created_at_raw)
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: DateTime::parse_from_rfc3339(&updated_at_raw)
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
    })
}

fn sqlite_err(err: rusqlite::Error) -> MemoryError {
    MemoryError::Store(format!("maintenance sqlite error: {err}"))
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
        source: None,
        source_ref: None,
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

    #[test]
    fn durable_queue_persists_candidates_and_status_after_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("maintenance.db");
        let queue = MaintenanceQueue::open_sqlite(&db).unwrap();
        let mut candidate = new_candidate(
            MaintenanceCandidateKind::Conflict,
            Vec::new(),
            "Review conflict".to_string(),
            "agents disagree".to_string(),
            0.81,
        );
        candidate.source = Some("collaboration_board".to_string());
        candidate.source_ref = Some("board-1".to_string());
        let id = candidate.id.clone();

        assert_eq!(queue.upsert_many(vec![candidate]).unwrap(), 1);
        let updated = queue
            .transition(&id, MaintenanceCandidateStatus::Acknowledged)
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, MaintenanceCandidateStatus::Acknowledged);

        let reopened = MaintenanceQueue::open_sqlite(&db).unwrap();
        let candidates = reopened
            .list(MaintenanceCandidateFilter {
                status: Some(MaintenanceCandidateStatus::Acknowledged),
                source: Some("collaboration_board".to_string()),
                ..MaintenanceCandidateFilter::default()
            })
            .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, id);
        assert_eq!(candidates[0].source_ref.as_deref(), Some("board-1"));
    }

    #[test]
    fn large_scan_respects_candidate_cap() {
        let entries = (0..2_000)
            .map(|index| {
                let mut item = entry(
                    &format!("Old decision {index}"),
                    "Historical implementation note",
                );
                item.staleness = 0.99;
                item
            })
            .collect::<Vec<_>>();

        let candidates = scan_maintenance_candidates(
            &entries,
            &MaintenanceScanConfig {
                max_candidates: 25,
                ..MaintenanceScanConfig::default()
            },
        );

        assert_eq!(candidates.len(), 25);
        assert!(candidates
            .iter()
            .all(|candidate| candidate.kind == MaintenanceCandidateKind::Stale));
    }
}
