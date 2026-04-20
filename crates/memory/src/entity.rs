//! Entity detection and knowledge graph for memory system.
//!
//! Borrowed from MemPalace entity_detector.py: dual-pass detection with
//! signal scoring, frequency thresholding, and time-bounded relationships.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Entity types
// ---------------------------------------------------------------------------

/// Type of detected entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Person,
    Project,
    Tool,
    Organization,
    Location,
    Concept,
}

impl std::fmt::Display for EntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityType::Person => write!(f, "Person"),
            EntityType::Project => write!(f, "Project"),
            EntityType::Tool => write!(f, "Tool"),
            EntityType::Organization => write!(f, "Organization"),
            EntityType::Location => write!(f, "Location"),
            EntityType::Concept => write!(f, "Concept"),
        }
    }
}

/// A detected entity with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: String,
    pub name: String,
    pub entity_type: EntityType,
    pub confidence: f64,
    pub frequency: usize,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub source_ids: Vec<String>,
}

/// A triple in the knowledge graph (subject-predicate-object).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Triple {
    pub id: String,
    pub subject_id: String,
    pub predicate: String,
    pub object_id: String,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    pub source: Option<String>,
    pub confidence: f64,
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Entity Detector
// ---------------------------------------------------------------------------

/// Signal patterns for entity detection (borrowed from MemPalace dual-pass).
const PERSON_PATTERNS: &[&str] = &[
    "(?i)(by|said|和|跟|与)\\s+([A-Z][a-z]+|[\\u4e00-\\u9fa5]{2,4})",
    "(?i)@([\\w]+)",
];

const PROJECT_PATTERNS: &[&str] = &[
    "(?i)(project|repo|repository|项目|仓库)\\s+([A-Za-z0-9_-]+)",
];

const TOOL_PATTERNS: &[&str] = &[
    "(?i)(using|with|用|使用|基于)\\s+([A-Za-z0-9_.-]+)",
    "(?i)(framework|library|库|框架)\\s+([A-Za-z0-9_.-]+)",
];

/// Dual-pass entity detector.
///
/// Pass 1: Extract candidate entities using signal patterns.
/// Pass 2: Classify and score candidates based on frequency.
pub struct EntityDetector {
    person_patterns: Vec<regex::Regex>,
    project_patterns: Vec<regex::Regex>,
    tool_patterns: Vec<regex::Regex>,
    /// Minimum frequency to confirm an entity (borrowed from MemPalace: >= 3).
    frequency_threshold: usize,
}

impl EntityDetector {
    pub fn new() -> Self {
        Self {
            person_patterns: PERSON_PATTERNS
                .iter()
                .filter_map(|p| regex::Regex::new(p).ok())
                .collect(),
            project_patterns: PROJECT_PATTERNS
                .iter()
                .filter_map(|p| regex::Regex::new(p).ok())
                .collect(),
            tool_patterns: TOOL_PATTERNS
                .iter()
                .filter_map(|p| regex::Regex::new(p).ok())
                .collect(),
            frequency_threshold: 2,
        }
    }

    /// First pass: Extract candidate entities from text.
    pub fn extract(&self, text: &str) -> Vec<(String, EntityType, f64)> {
        let mut candidates: Vec<(String, EntityType, f64)> = Vec::new();

        // Person entities
        for re in &self.person_patterns {
            for cap in re.captures_iter(text) {
                if let Some(m) = cap.get(2).or_else(|| cap.get(1)) {
                    let name = m.as_str().trim().to_string();
                    if !name.is_empty() && name.len() >= 2 {
                        candidates.push((name, EntityType::Person, 0.6));
                    }
                }
            }
        }

        // Project entities
        for re in &self.project_patterns {
            for cap in re.captures_iter(text) {
                if let Some(m) = cap.get(2) {
                    let name = m.as_str().trim().to_string();
                    if !name.is_empty() && name.len() >= 2 {
                        candidates.push((name, EntityType::Project, 0.7));
                    }
                }
            }
        }

        // Tool entities
        for re in &self.tool_patterns {
            for cap in re.captures_iter(text) {
                if let Some(m) = cap.get(2) {
                    let name = m.as_str().trim().to_string();
                    if !name.is_empty() && name.len() >= 2 {
                        candidates.push((name, EntityType::Tool, 0.7));
                    }
                }
            }
        }

        candidates
    }

    /// Second pass: Classify and filter candidates based on frequency.
    pub fn classify(
        &self,
        candidates: &[(String, EntityType, f64)],
        frequency_map: &HashMap<String, usize>,
    ) -> Vec<Entity> {
        let now = Utc::now();
        let mut seen: HashMap<(String, EntityType), Entity> = HashMap::new();

        for (name, etype, confidence) in candidates {
            let key = (name.clone(), *etype);
            let freq = frequency_map.get(name).copied().unwrap_or(1);

            // Apply frequency threshold
            if freq < self.frequency_threshold {
                continue;
            }

            let adjusted_confidence = confidence * (1.0 + 0.1 * freq.min(10) as f64).min(2.0);

            let entity = Entity {
                id: format!("{}-{}-{}", etype, name, uuid::Uuid::new_v4().as_simple()),
                name: name.clone(),
                entity_type: *etype,
                confidence: adjusted_confidence.min(1.0),
                frequency: freq,
                first_seen: now,
                last_seen: now,
                source_ids: Vec::new(),
            };

            seen.entry(key)
                .and_modify(|e| {
                    e.confidence = e.confidence.max(adjusted_confidence.min(1.0));
                    e.frequency = e.frequency.max(freq);
                })
                .or_insert(entity);
        }

        seen.into_values().collect()
    }
}

impl Default for EntityDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Knowledge Graph (in-memory with optional SQLite persistence)
// ---------------------------------------------------------------------------

/// In-memory knowledge graph for entity relationships.
pub struct KnowledgeGraph {
    entities: HashMap<String, Entity>,
    triples: Vec<Triple>,
}

impl KnowledgeGraph {
    pub fn new() -> Self {
        Self {
            entities: HashMap::new(),
            triples: Vec::new(),
        }
    }

    /// Add or update an entity. Returns the entity ID.
    pub fn add_entity(&mut self, entity: Entity) -> String {
        let id = entity.id.clone();
        self.entities.insert(id.clone(), entity);
        id
    }

    /// Get entity by name (case-insensitive).
    pub fn get_entity_by_name(&self, name: &str) -> Option<&Entity> {
        let lower = name.to_lowercase();
        self.entities.values().find(|e| e.name.to_lowercase() == lower)
    }

    /// Get entity by ID.
    pub fn get_entity(&self, id: &str) -> Option<&Entity> {
        self.entities.get(id)
    }

    /// Add a triple (subject-predicate-object) relationship.
    pub fn add_triple(
        &mut self,
        subject_id: &str,
        predicate: &str,
        object_id: &str,
        valid_from: Option<DateTime<Utc>>,
        source: Option<String>,
        confidence: f64,
    ) -> String {
        let triple = Triple {
            id: uuid::Uuid::new_v4().to_string(),
            subject_id: subject_id.to_string(),
            predicate: predicate.to_string(),
            object_id: object_id.to_string(),
            valid_from,
            valid_to: None,
            source,
            confidence,
            created_at: Utc::now(),
        };
        let id = triple.id.clone();
        self.triples.push(triple);
        id
    }

    /// Query triples by subject, predicate, or object (any can be None for wildcard).
    pub fn query_triples(
        &self,
        subject_id: Option<&str>,
        predicate: Option<&str>,
        object_id: Option<&str>,
    ) -> Vec<&Triple> {
        self.triples
            .iter()
            .filter(|t| {
                subject_id.map_or(true, |s| t.subject_id == s)
                    && predicate.map_or(true, |p| t.predicate == p)
                    && object_id.map_or(true, |o| t.object_id == o)
                    && t.valid_to.is_none() // Only return currently valid triples
            })
            .collect()
    }

    /// Expire a triple by setting its valid_to timestamp.
    pub fn expire_triple(&mut self, id: &str, valid_to: DateTime<Utc>) {
        if let Some(t) = self.triples.iter_mut().find(|t| t.id == id) {
            t.valid_to = Some(valid_to);
        }
    }

    /// List all entities.
    pub fn list_entities(&self) -> Vec<&Entity> {
        self.entities.values().collect()
    }

    /// List all valid triples.
    pub fn list_triples(&self) -> Vec<&Triple> {
        self.triples.iter().filter(|t| t.valid_to.is_none()).collect()
    }

    /// Get entities related to a given entity.
    pub fn get_related(&self, entity_id: &str) -> Vec<(&Entity, &str, &Entity)> {
        let mut results = Vec::new();
        for t in &self.triples {
            if t.valid_to.is_some() {
                continue;
            }
            if t.subject_id == entity_id {
                if let Some(obj) = self.entities.get(&t.object_id) {
                    results.push((obj, t.predicate.as_str(), obj));
                }
            } else if t.object_id == entity_id {
                if let Some(subj) = self.entities.get(&t.subject_id) {
                    results.push((subj, t.predicate.as_str(), subj));
                }
            }
        }
        results
    }
}

impl Default for KnowledgeGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_detector_extract() {
        let detector = EntityDetector::new();
        let text = "by John and using React framework in project cowd";
        let candidates = detector.extract(text);
        assert!(!candidates.is_empty(), "Should detect entities");

        let types: Vec<_> = candidates.iter().map(|(_, t, _)| t).collect();
        assert!(types.iter().any(|t| **t == EntityType::Person), "Should detect person");
        assert!(types.iter().any(|t| **t == EntityType::Tool), "Should detect tool");
        assert!(types.iter().any(|t| **t == EntityType::Project), "Should detect project");
    }

    #[test]
    fn test_entity_detector_classify_frequency() {
        let detector = EntityDetector::new();
        let candidates = vec![
            ("React".to_string(), EntityType::Tool, 0.7),
            ("React".to_string(), EntityType::Tool, 0.7),
        ];
        let mut freq = HashMap::new();
        freq.insert("React".to_string(), 3);
        let entities = detector.classify(&candidates, &freq);
        assert!(!entities.is_empty(), "React should pass frequency threshold");
    }

    #[test]
    fn test_knowledge_graph_add_query() {
        let mut kg = KnowledgeGraph::new();

        let person = Entity {
            id: "p1".to_string(),
            name: "Alice".to_string(),
            entity_type: EntityType::Person,
            confidence: 0.9,
            frequency: 5,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            source_ids: vec![],
        };
        kg.add_entity(person);

        let project = Entity {
            id: "proj1".to_string(),
            name: "cowd".to_string(),
            entity_type: EntityType::Project,
            confidence: 0.8,
            frequency: 10,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            source_ids: vec![],
        };
        kg.add_entity(project);

        kg.add_triple("p1", "works_on", "proj1", None, None, 0.9);

        let results = kg.query_triples(Some("p1"), None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].predicate, "works_on");
    }

    #[test]
    fn test_knowledge_graph_expire() {
        let mut kg = KnowledgeGraph::new();
        let e = Entity {
            id: "e1".to_string(),
            name: "Test".to_string(),
            entity_type: EntityType::Concept,
            confidence: 0.5,
            frequency: 1,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            source_ids: vec![],
        };
        kg.add_entity(e);
        let tid = kg.add_triple("e1", "relates_to", "e1", None, None, 0.5);

        // Before expiry
        assert_eq!(kg.list_triples().len(), 1);

        // Expire
        kg.expire_triple(&tid, Utc::now());

        // After expiry
        assert_eq!(kg.list_triples().len(), 0);
    }
}
