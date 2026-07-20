//! Temporal Knowledge Graph for long-term memory relationships.
//!
//! Inspired by MemPalace's knowledge_graph.py, this module provides:
//! - Entity nodes (people, projects, tools, concepts)
//! - Typed relationship edges (Subject → Predicate → Object)
//! - Temporal validity (valid_from → valid_to — knows WHEN facts are true)
//! - Source tracking (links back to verbatim memory)
//!
//! Usage:
//!     let kg = KnowledgeGraph::new(store);
//!     kg.add_entity("Alice", EntityType::Person)?;
//!     kg.add_triple("Alice", "child_of", "Bob", valid_from)?;
//!     kg.query_entity("Alice", QueryOptions::default())?;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::store::MemoryStore;
use crate::types::{MemoryId, Relation, RelationKind, TemporalMarker};

// ─── Entity Types ─────────────────────────────────────────────────────────────

/// Entity types for knowledge graph nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Person,
    Project,
    Tool,
    Concept,
    Location,
    Organization,
    Unknown,
}

/// A node in the knowledge graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: String,
    pub name: String,
    pub entity_type: EntityType,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// A triple: Subject → Predicate → Object (like RDF)
///
/// Inspired by MemPalace's knowledge_graph.py triple structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Triple {
    pub id: String,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub confidence: f32,
    /// Memory entry ID this fact was extracted from (like MemPalace's source_closet)
    pub source_memory_id: Option<MemoryId>,
    /// Source file path (like MemPalace's source_file)
    pub source_file: Option<String>,
    /// Agent that produced this triple (for cross-agent attribution).
    pub source_agent: Option<String>,
}

/// Entity facts for seeding the knowledge graph.
///
/// Similar to MemPalace's fact_checker.py ENTITY_FACTS structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EntityFacts {
    /// Full name of the entity.
    pub full_name: Option<String>,
    /// Entity type (person, project, tool, concept, organization).
    pub entity_type: Option<String>,
    /// Parent entity name (for person type).
    pub parent: Option<String>,
    /// Partner entity name.
    pub partner: Option<String>,
    /// Birthday for temporal relationship.
    pub birthday: Option<DateTime<Utc>>,
    /// Interests list.
    pub interests: Vec<String>,
    /// Custom relationships (predicate -> object).
    pub relationships: Vec<(String, String)>,
}

// ─── Query Options ────────────────────────────────────────────────────────────

/// Query direction for entity traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryDirection {
    Outgoing, // entity → ?
    Incoming, // ? → entity
    Both,
}

/// Options for entity queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptions {
    pub direction: QueryDirection,
    pub as_of: Option<DateTime<Utc>>,
    pub include_invalidated: bool,
    pub max_results: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            direction: QueryDirection::Outgoing,
            as_of: None,
            include_invalidated: false,
            max_results: 100,
        }
    }
}

/// Result of an entity query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityQueryResult {
    pub direction: String,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub confidence: f32,
    pub current: bool,
    /// Source memory ID (like MemPalace's source_closet)
    pub source_memory_id: Option<MemoryId>,
}

// ─── Knowledge Graph ───────────────────────────────────────────────────────────

/// A temporal entity-relationship knowledge graph.
/// Similar to MemPalace's KnowledgeGraph class.
pub struct KnowledgeGraph {
    /// In-memory entity cache.
    entities: HashMap<String, Entity>,
    /// In-memory triple cache.
    triples: Vec<Triple>,
}

impl KnowledgeGraph {
    /// Create a new knowledge graph.
    pub fn new(_store: Arc<dyn MemoryStore>) -> Self {
        Self {
            entities: HashMap::new(),
            triples: Vec::new(),
        }
    }

    /// Add an entity node.
    pub fn add_entity(&mut self, name: &str, entity_type: EntityType) -> String {
        let id = Self::entity_id(name);
        let entity = Entity {
            id: id.clone(),
            name: name.to_string(),
            entity_type,
            properties: HashMap::new(),
            created_at: Utc::now(),
        };
        self.entities.insert(id.clone(), entity);
        id
    }

    /// Add a relationship triple with source tracking.
    ///
    /// Similar to MemPalace's `add_triple()`:
    ///     kg.add_triple("Alice", "child_of", "Bob", valid_from="2015-04-01")
    ///
    /// # Arguments
    /// * `subject` - Source entity name
    /// * `predicate` - Relationship type (e.g., "child_of", "works_on")
    /// * `object` - Target entity name
    /// * `valid_from` - When this fact became true
    /// * `valid_until` - When this fact stopped being true
    /// * `source_memory_id` - Memory entry ID this fact was extracted from
    /// * `source_file` - Source file path
    pub fn add_triple(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        valid_from: Option<DateTime<Utc>>,
        valid_until: Option<DateTime<Utc>>,
        source_memory_id: Option<MemoryId>,
        source_file: Option<String>,
    ) -> String {
        // Auto-create entities if they don't exist
        let sub_id = self.add_entity(subject, EntityType::Unknown);
        let obj_id = self.add_entity(object, EntityType::Unknown);
        let pred = predicate.to_lowercase().replace(' ', "_");

        // Check for existing identical triple
        let exists = self.triples.iter().any(|t| {
            t.subject == sub_id
                && t.predicate == pred
                && t.object == obj_id
                && t.valid_until.is_none()
        });

        if exists {
            return format!("{}_{}_{}", sub_id, pred, obj_id);
        }

        let triple_id = format!(
            "t_{}_{}_{}_{}",
            sub_id,
            pred,
            obj_id,
            Utc::now().timestamp()
        );

        self.triples.push(Triple {
            id: triple_id.clone(),
            subject: sub_id,
            predicate: pred,
            object: obj_id,
            valid_from,
            valid_until,
            confidence: 1.0,
            source_memory_id,
            source_file,
            source_agent: None,
        });

        triple_id
    }

    /// Add a relationship triple (simplified version without source tracking).
    ///
    /// For backward compatibility, use this for simple triples without source info.
    pub fn add_triple_simple(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        valid_from: Option<DateTime<Utc>>,
        valid_until: Option<DateTime<Utc>>,
    ) -> String {
        self.add_triple(
            subject,
            predicate,
            object,
            valid_from,
            valid_until,
            None,
            None,
        )
    }

    /// Invalidate a relationship (set valid_until).
    ///
    /// Similar to MemPalace's `invalidate()`:
    ///     kg.invalidate("Max", "has_issue", "injury", ended="2026-02-15")
    pub fn invalidate(&mut self, subject: &str, predicate: &str, object: &str) {
        let sub_id = Self::entity_id(subject);
        let obj_id = Self::entity_id(object);
        let pred = predicate.to_lowercase().replace(' ', "_");

        for triple in self.triples.iter_mut().rev() {
            if triple.subject == sub_id
                && triple.predicate == pred
                && triple.object == obj_id
                && triple.valid_until.is_none()
            {
                triple.valid_until = Some(Utc::now());
            }
        }
    }

    /// Return a reference to all triples in the graph.
    pub fn all_triples(&self) -> &[Triple] {
        &self.triples
    }

    /// Query all relationships for an entity.
    ///
    /// Similar to MemPalace's `query_entity()`:
    ///     kg.query_entity("Max", as_of="2026-01-15", direction="both")
    pub fn query_entity(&self, name: &str, options: &QueryOptions) -> Vec<EntityQueryResult> {
        let eid = Self::entity_id(name);
        let mut results = Vec::new();

        match options.direction {
            QueryDirection::Outgoing | QueryDirection::Both => {
                for triple in &self.triples {
                    if triple.subject == eid && self.triple_is_valid(triple, options) {
                        let obj_name = self
                            .entities
                            .get(&triple.object)
                            .map(|e| e.name.clone())
                            .unwrap_or_else(|| triple.object.clone());

                        results.push(EntityQueryResult {
                            direction: "outgoing".to_string(),
                            subject: name.to_string(),
                            predicate: triple.predicate.clone(),
                            object: obj_name,
                            valid_from: triple.valid_from,
                            valid_until: triple.valid_until,
                            confidence: triple.confidence,
                            current: triple.valid_until.is_none(),
                            source_memory_id: triple.source_memory_id,
                        });
                    }
                }
            }
            QueryDirection::Incoming => {}
        }

        if matches!(
            options.direction,
            QueryDirection::Both | QueryDirection::Incoming
        ) {
            for triple in &self.triples {
                if triple.object == eid && self.triple_is_valid(triple, options) {
                    let sub_name = self
                        .entities
                        .get(&triple.subject)
                        .map(|e| e.name.clone())
                        .unwrap_or_else(|| triple.subject.clone());

                    results.push(EntityQueryResult {
                        direction: "incoming".to_string(),
                        subject: sub_name,
                        predicate: triple.predicate.clone(),
                        object: name.to_string(),
                        valid_from: triple.valid_from,
                        valid_until: triple.valid_until,
                        confidence: triple.confidence,
                        current: triple.valid_until.is_none(),
                        source_memory_id: triple.source_memory_id,
                    });
                }
            }
        }

        results.truncate(options.max_results);
        results
    }

    /// Get timeline of facts in chronological order.
    pub fn timeline(&self, entity_name: Option<&str>) -> Vec<EntityQueryResult> {
        let mut results: Vec<EntityQueryResult> = Vec::new();

        let triples: Vec<&Triple> = if let Some(name) = entity_name {
            let eid = Self::entity_id(name);
            self.triples
                .iter()
                .filter(|t| t.subject == eid || t.object == eid)
                .collect()
        } else {
            self.triples.iter().collect()
        };

        // Sort by valid_from timestamp
        let mut sorted: Vec<_> = triples.into_iter().collect();
        sorted.sort_by(|a, b| a.valid_from.cmp(&b.valid_from));

        for triple in sorted {
            let sub_name = self
                .entities
                .get(&triple.subject)
                .map(|e| e.name.clone())
                .unwrap_or_else(|| triple.subject.clone());
            let obj_name = self
                .entities
                .get(&triple.object)
                .map(|e| e.name.clone())
                .unwrap_or_else(|| triple.object.clone());

            results.push(EntityQueryResult {
                direction: "timeline".to_string(),
                subject: sub_name,
                predicate: triple.predicate.clone(),
                object: obj_name,
                valid_from: triple.valid_from,
                valid_until: triple.valid_until,
                confidence: triple.confidence,
                current: triple.valid_until.is_none(),
                source_memory_id: triple.source_memory_id,
            });
        }

        results
    }

    /// Query all triples with a given relationship type.
    ///
    /// Similar to MemPalace's `query_relationship()`:
    ///     kg.query_relationship("child_of", as_of="2026-01-15")
    pub fn query_relationship(
        &self,
        predicate: &str,
        as_of: Option<DateTime<Utc>>,
    ) -> Vec<EntityQueryResult> {
        let pred = predicate.to_lowercase().replace(' ', "_");
        let mut results = Vec::new();

        for triple in &self.triples {
            if triple.predicate != pred {
                continue;
            }

            // Check temporal validity
            if let Some(as_of_time) = as_of {
                if let Some(valid_from) = triple.valid_from {
                    if valid_from > as_of_time {
                        continue;
                    }
                }
                if let Some(valid_until) = triple.valid_until {
                    if valid_until < as_of_time {
                        continue;
                    }
                }
            } else if triple.valid_until.is_some() {
                // No as_of filter, skip invalidated if not requested
                continue;
            }

            let sub_name = self
                .entities
                .get(&triple.subject)
                .map(|e| e.name.clone())
                .unwrap_or_else(|| triple.subject.clone());
            let obj_name = self
                .entities
                .get(&triple.object)
                .map(|e| e.name.clone())
                .unwrap_or_else(|| triple.object.clone());

            results.push(EntityQueryResult {
                direction: "relationship".to_string(),
                subject: sub_name,
                predicate: triple.predicate.clone(),
                object: obj_name,
                valid_from: triple.valid_from,
                valid_until: triple.valid_until,
                confidence: triple.confidence,
                current: triple.valid_until.is_none(),
                source_memory_id: triple.source_memory_id,
            });
        }

        results
    }

    /// Get all relationship types (predicates) in the graph.
    ///
    /// Similar to MemPalace's stats()["relationship_types"].
    pub fn relationship_types(&self) -> Vec<String> {
        let mut predicates: std::collections::HashSet<String> = std::collections::HashSet::new();
        for triple in &self.triples {
            predicates.insert(triple.predicate.clone());
        }
        let mut result: Vec<String> = predicates.into_iter().collect();
        result.sort();
        result
    }

    /// Get graph statistics.
    pub fn stats(&self) -> KnowledgeGraphStats {
        let current_triples = self
            .triples
            .iter()
            .filter(|t| t.valid_until.is_none())
            .count();
        let historical_triples = self.triples.len() - current_triples;

        KnowledgeGraphStats {
            total_entities: self.entities.len(),
            total_triples: self.triples.len(),
            current_triples,
            historical_triples,
            earliest_fact: self.triples.iter().filter_map(|t| t.valid_from).min(),
            latest_fact: self.triples.iter().filter_map(|t| t.valid_from).max(),
        }
    }

    /// Seed the knowledge graph from known entity facts.
    ///
    /// Similar to MemPalace's `seed_from_entity_facts()`:
    ///     kg.seed_from_entity_facts({
    ///         "max": {"full_name": "Max", "type": "person", "parent": "Alice"}
    ///     })
    ///
    /// # Arguments
    /// * `entity_facts` - HashMap of entity name to fact struct
    pub fn seed_from_entity_facts(&mut self, entity_facts: &HashMap<String, EntityFacts>) {
        for (key, facts) in entity_facts {
            let name = facts.full_name.as_ref().unwrap_or(key);
            let etype = facts
                .entity_type
                .as_ref()
                .map(|t| match t.as_str() {
                    "person" => EntityType::Person,
                    "project" => EntityType::Project,
                    "tool" => EntityType::Tool,
                    "concept" => EntityType::Concept,
                    "organization" => EntityType::Organization,
                    "animal" => EntityType::Unknown,
                    _ => EntityType::Unknown,
                })
                .unwrap_or(EntityType::Unknown);

            // Add entity
            self.add_entity(name, etype);

            // Parent relationship
            if let Some(parent) = &facts.parent {
                self.add_triple_simple(name, "child_of", parent, facts.birthday, None);
            }

            // Partner relationship
            if let Some(partner) = &facts.partner {
                self.add_triple_simple(name, "married_to", partner, None, None);
            }

            // Interest relationships
            for interest in &facts.interests {
                self.add_triple_simple(name, "loves", interest, None, None);
            }

            // Custom relationships
            for (pred, obj) in &facts.relationships {
                self.add_triple_simple(name, pred, obj, None, None);
            }
        }
    }

    /// Helper to check if triple is valid at given time.
    fn triple_is_valid(&self, triple: &Triple, options: &QueryOptions) -> bool {
        if !options.include_invalidated && triple.valid_until.is_some() {
            return false;
        }

        if let Some(as_of) = options.as_of {
            if let Some(valid_from) = triple.valid_from {
                if valid_from > as_of {
                    return false;
                }
            }
            if let Some(valid_until) = triple.valid_until {
                if valid_until < as_of {
                    return false;
                }
            }
        }

        true
    }

    /// Generate entity ID from name.
    fn entity_id(name: &str) -> String {
        name.to_lowercase().replace(' ', "_").replace('\'', "")
    }
}

/// Statistics about the knowledge graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeGraphStats {
    pub total_entities: usize,
    pub total_triples: usize,
    pub current_triples: usize,
    pub historical_triples: usize,
    pub earliest_fact: Option<DateTime<Utc>>,
    pub latest_fact: Option<DateTime<Utc>>,
}

// ─── Re-exports for backward compatibility ────────────────────────────────────

/// Time range for temporal queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

impl TimeRange {
    pub fn new(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Self {
        Self { start, end }
    }

    pub fn before(until: DateTime<Utc>) -> Self {
        Self {
            start: None,
            end: Some(until),
        }
    }

    pub fn after(start: DateTime<Utc>) -> Self {
        Self {
            start: Some(start),
            end: None,
        }
    }

    pub fn between(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            start: Some(start),
            end: Some(end),
        }
    }

    /// Check if this range contains the given time.
    pub fn contains(&self, time: DateTime<Utc>) -> bool {
        if let Some(start) = self.start {
            if time < start {
                return false;
            }
        }
        if let Some(end) = self.end {
            if time > end {
                return false;
            }
        }
        true
    }

    /// Create from days ago to now.
    pub fn days_ago(days: i64) -> Self {
        Self {
            start: Some(Utc::now() - Duration::days(days)),
            end: Some(Utc::now()),
        }
    }
}

/// Helper to create a temporal relation.
pub fn temporal_relation(target_id: MemoryId, kind: RelationKind, strength: f32) -> Relation {
    Relation {
        target_id,
        kind,
        strength,
        temporal: Some(TemporalMarker {
            established_at: Utc::now(),
            valid_from: None,
            valid_until: None,
            sequence: 0,
        }),
        entity: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // Test pure functions that don't require a store

    #[test]
    fn test_entity_type_serialization() {
        let et = EntityType::Person;
        let json = serde_json::to_string(&et).unwrap();
        assert!(json.contains("Person"));
        let parsed: EntityType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, et);
    }

    #[test]
    fn test_query_options_default() {
        let opts = QueryOptions::default();
        assert_eq!(opts.direction, QueryDirection::Outgoing);
        assert!(!opts.include_invalidated);
        assert_eq!(opts.max_results, 100);
    }

    #[test]
    fn test_query_direction() {
        assert!(matches!(QueryDirection::Outgoing, QueryDirection::Outgoing));
        assert!(matches!(QueryDirection::Incoming, QueryDirection::Incoming));
        assert!(matches!(QueryDirection::Both, QueryDirection::Both));
    }

    #[test]
    fn test_time_range() {
        let range = TimeRange::days_ago(7);
        assert!(range.start.is_some());
        assert!(range.end.is_some());
    }

    #[test]
    fn test_time_range_before() {
        let now = Utc::now();
        let range = TimeRange::before(now);
        assert!(range.start.is_none());
        assert_eq!(range.end, Some(now));
        assert!(range.contains(now));
    }

    #[test]
    fn test_time_range_after() {
        let now = Utc::now();
        let range = TimeRange::after(now);
        assert_eq!(range.start, Some(now));
        assert!(range.end.is_none());
        assert!(range.contains(now));
    }

    #[test]
    fn test_time_range_between() {
        let start = Utc::now() - Duration::days(7);
        let end = Utc::now();
        let range = TimeRange::between(start, end);
        assert_eq!(range.start, Some(start));
        assert_eq!(range.end, Some(end));
        assert!(range.contains(Utc::now() - Duration::days(3)));
    }

    #[test]
    fn test_time_range_contains_false() {
        let start = Utc::now() - Duration::days(7);
        let end = Utc::now() - Duration::days(1);
        let range = TimeRange::between(start, end);
        assert!(!range.contains(Utc::now())); // Now is after range
    }

    #[test]
    fn test_temporal_relation_fn() {
        let id = Uuid::new_v4();
        let rel = temporal_relation(id, RelationKind::Before, 0.8);
        assert_eq!(rel.target_id, id);
        assert!(matches!(rel.kind, RelationKind::Before));
        assert!(rel.temporal.is_some());
        assert_eq!(rel.strength, 0.8);
    }

    #[test]
    fn test_temporal_relation_with_dates() {
        let id = Uuid::new_v4();
        let valid_from = Utc::now() - Duration::days(30);
        let valid_until = Utc::now();

        let rel = Relation {
            target_id: id,
            kind: RelationKind::Concurrent,
            strength: 0.9,
            temporal: Some(TemporalMarker {
                established_at: valid_from,
                valid_from: Some(valid_from),
                valid_until: Some(valid_until),
                sequence: 1,
            }),
            entity: None,
        };

        assert!(rel.temporal.is_some());
        let tm = rel.temporal.unwrap();
        assert_eq!(tm.sequence, 1);
    }

    #[test]
    fn test_graph_stats_serialization() {
        let stats = KnowledgeGraphStats {
            total_entities: 10,
            total_triples: 25,
            current_triples: 20,
            historical_triples: 5,
            earliest_fact: Some(Utc::now() - Duration::days(30)),
            latest_fact: Some(Utc::now()),
        };
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("10"));
        let parsed: KnowledgeGraphStats = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.total_entities, 10);
    }

    #[test]
    fn test_entity_query_result() {
        let result = EntityQueryResult {
            direction: "outgoing".to_string(),
            subject: "Alice".to_string(),
            predicate: "knows".to_string(),
            object: "Bob".to_string(),
            valid_from: Some(Utc::now()),
            valid_until: None,
            confidence: 1.0,
            current: true,
            source_memory_id: None,
        };
        assert_eq!(result.direction, "outgoing");
        assert!(result.current);
        assert_eq!(result.confidence, 1.0);
    }

    // ========================================================================
    // MemPalace Compatible Tests (simplified)
    // ========================================================================

    #[test]
    fn test_predicate_normalization() {
        let pred = "child_of".to_lowercase().replace(' ', "_");
        assert_eq!(pred, "child_of");

        let pred2 = "works on".to_lowercase().replace(' ', "_");
        assert_eq!(pred2, "works_on");
    }

    #[test]
    fn test_entity_facts_default() {
        let facts = EntityFacts::default();
        assert!(facts.full_name.is_none());
        assert!(facts.interests.is_empty());
        assert!(facts.relationships.is_empty());
    }

    #[test]
    fn test_triple_source_fields() {
        let triple = Triple {
            id: "test".to_string(),
            subject: "Max".to_string(),
            predicate: "loves".to_string(),
            object: "Rust".to_string(),
            valid_from: None,
            valid_until: None,
            confidence: 1.0,
            source_memory_id: None,
            source_file: Some("/path/to/file.md".to_string()),
            source_agent: None,
        };
        assert!(triple.source_file.is_some());
        assert_eq!(triple.source_file.unwrap(), "/path/to/file.md");
    }
}
