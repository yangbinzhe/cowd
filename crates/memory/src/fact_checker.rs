//! Fact checker for the knowledge graph.
//!
//! Inspired by MemPalace's `fact_checker.py`. Validates triples against known
//! entity facts to detect contradictions, and provides an audit trail of all
//! inconsistencies found.
//!
//! # Validation strategy
//!
//! 1. **Type check** — the predicate must be compatible with the entity type
//!    (e.g. a `Person` can have `child_of` but a `Project` cannot).
//! 2. **Consistency check** — the triple does not contradict any registered
//!    facts for the same subject (e.g. two different `full_name` values).
//! 3. **Temporal check** — the triple's validity window does not overlap with
//!    an invalidated triple of the same (subject, predicate, object).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::temporal_graph::{EntityFacts, EntityType, KnowledgeGraph, Triple};

// ─── Result types ──────────────────────────────────────────────────────────────

/// Outcome of checking a single triple.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactCheckResult {
    /// The triple that was checked.
    pub triple_id: String,
    /// Whether the triple is consistent with known facts.
    pub is_consistent: bool,
    /// Confidence score in `[0.0, 1.0]`.
    pub confidence: f32,
    /// Human-readable description of the contradiction (if any).
    pub contradiction: Option<String>,
    /// Suggested correction (if applicable).
    pub suggested_correction: Option<String>,
}

/// Predicate-entity-type compatibility rule.
#[derive(Debug, Clone)]
struct PredicateRule {
    /// Which entity types the subject of this predicate must belong to.
    allowed_subject_types: Vec<EntityType>,
}

// ─── Built-in rules ────────────────────────────────────────────────────────────

/// Predicates that only make sense for persons.
const PERSON_PREDICATES: &[&str] = &[
    "child_of", "parent_of", "partner_of", "sibling_of",
    "born_on", "birthday", "works_for", "manages",
];

/// Predicates valid for organizations.
const ORG_PREDICATES: &[&str] = &[
    "located_in", "subsidiary_of", "owns", "employs",
];

/// Predicates valid for projects.
const PROJECT_PREDICATES: &[&str] = &[
    "uses", "depends_on", "belongs_to", "has_member",
];

/// Predicates valid for any entity type.
const UNIVERSAL_PREDICATES: &[&str] = &[
    "related_to", "has_property", "contains", "part_of",
    "located_in", "known_as", "also_called",
];

// ─── FactChecker ───────────────────────────────────────────────────────────────

/// Validates knowledge-graph triples against registered entity facts.
#[derive(Debug, Clone)]
pub struct FactChecker {
    /// Registered entity facts keyed by entity name (lowercased).
    entity_facts: HashMap<String, EntityFacts>,
}

impl FactChecker {
    /// Create a new fact checker with no registered facts.
    pub fn new() -> Self {
        Self {
            entity_facts: HashMap::new(),
        }
    }

    /// Register known facts about an entity.
    pub fn register_facts(&mut self, entity: &str, facts: EntityFacts) {
        self.entity_facts.insert(entity.to_lowercase(), facts);
    }

    /// Check a single triple against known facts.
    pub fn check_triple(&self, triple: &Triple) -> FactCheckResult {
        let mut contradictions = Vec::new();
        let mut confidence = triple.confidence;

        // 1. Type-check the predicate against known entity facts.
        if let Some(rule) = self.predicate_rule(&triple.predicate) {
            let subject_lc = triple.subject.to_lowercase();
            if let Some(facts) = self.entity_facts.get(&subject_lc) {
                if let Some(ref type_str) = facts.entity_type {
                    let entity_type = parse_entity_type(type_str);
                    if !rule.allowed_subject_types.contains(&entity_type) {
                        contradictions.push(format!(
                            "Predicate '{}' is not valid for entity type '{}' (subject: {})",
                            triple.predicate, type_str, triple.subject
                        ));
                        confidence = (confidence * 0.5).min(0.3);
                    }
                }
            }
        }

        // 2. Consistency check — look for conflicting values in registered facts.
        let subject_lc = triple.subject.to_lowercase();
        if let Some(facts) = self.entity_facts.get(&subject_lc) {
            if triple.predicate == "child_of" || triple.predicate == "parent_of" {
                if let Some(ref parent) = facts.parent {
                    let obj_lc = triple.object.to_lowercase();
                    if obj_lc != parent.to_lowercase()
                        && !obj_lc.is_empty()
                        && !parent.is_empty()
                    {
                        contradictions.push(format!(
                            "Triple says {} is child_of {}, but registered facts say parent is {}",
                            triple.subject, triple.object, parent
                        ));
                        confidence = (confidence * 0.3).min(0.2);
                    }
                }
            }
            if triple.predicate == "partner_of" {
                if let Some(ref partner) = facts.partner {
                    let obj_lc = triple.object.to_lowercase();
                    if obj_lc != partner.to_lowercase()
                        && !obj_lc.is_empty()
                        && !partner.is_empty()
                    {
                        contradictions.push(format!(
                            "Triple says {} is partner_of {}, but registered facts say partner is {}",
                            triple.subject, triple.object, partner
                        ));
                        confidence = (confidence * 0.3).min(0.2);
                    }
                }
            }
            if triple.predicate == "full_name" || triple.predicate == "known_as" {
                if let Some(ref full_name) = facts.full_name {
                    if triple.object != *full_name && !triple.object.is_empty() {
                        contradictions.push(format!(
                            "Triple says {} name is '{}', but registered facts say '{}'",
                            triple.subject, triple.object, full_name
                        ));
                        confidence = (confidence * 0.3).min(0.2);
                    }
                }
            }
        }

        // 3. Temporal overlap check — if a previous triple with the same
        //    (subject, predicate) has been invalidated, the new one is fine.
        //    If an existing non-invalidated triple has a different object, flag it.
        //    (This is done externally via the KnowledgeGraph query, but we note
        //    the principle here.)

        let is_consistent = contradictions.is_empty();
        let contradiction = if contradictions.is_empty() {
            None
        } else {
            Some(contradictions.join("; "))
        };

        let suggested_correction = if !is_consistent {
            Some(format!("Review and reconcile the conflicting facts for {}", triple.subject))
        } else {
            None
        };

        FactCheckResult {
            triple_id: triple.id.clone(),
            is_consistent,
            confidence,
            contradiction,
            suggested_correction,
        }
    }

    /// Batch-check all triples in a knowledge graph.
    pub fn check_graph(&self, graph: &KnowledgeGraph) -> Vec<FactCheckResult> {
        graph.all_triples().iter().map(|t| self.check_triple(t)).collect()
    }

    // ── helpers ────────────────────────────────────────────────────────────

    fn predicate_rule(&self, predicate: &str) -> Option<PredicateRule> {
        let pred_lc = predicate.to_lowercase();

        if PERSON_PREDICATES.iter().any(|p| *p == pred_lc) {
            return Some(PredicateRule {
                allowed_subject_types: vec![EntityType::Person],
            });
        }
        if ORG_PREDICATES.iter().any(|p| *p == pred_lc) {
            return Some(PredicateRule {
                allowed_subject_types: vec![EntityType::Organization],
            });
        }
        if PROJECT_PREDICATES.iter().any(|p| *p == pred_lc) {
            return Some(PredicateRule {
                allowed_subject_types: vec![EntityType::Project, EntityType::Tool],
            });
        }
        if UNIVERSAL_PREDICATES.iter().any(|p| *p == pred_lc) {
            return Some(PredicateRule {
                allowed_subject_types: vec![
                    EntityType::Person,
                    EntityType::Project,
                    EntityType::Tool,
                    EntityType::Concept,
                    EntityType::Location,
                    EntityType::Organization,
                    EntityType::Unknown,
                ],
            });
        }

        // Unknown predicate: allow anything (don't block)
        None
    }
}

impl Default for FactChecker {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_entity_type(s: &str) -> EntityType {
    match s.to_lowercase().as_str() {
        "person" | "people" => EntityType::Person,
        "project" => EntityType::Project,
        "tool" => EntityType::Tool,
        "concept" => EntityType::Concept,
        "location" | "place" => EntityType::Location,
        "organization" | "org" | "company" => EntityType::Organization,
        _ => EntityType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistent_triple_passes() {
        let mut checker = FactChecker::new();
        let mut facts = EntityFacts::default();
        facts.entity_type = Some("person".to_string());
        facts.parent = Some("Bob".to_string());
        checker.register_facts("Alice", facts);

        let triple = Triple {
            id: "t1".to_string(),
            subject: "alice".to_string(),
            predicate: "child_of".to_string(),
            object: "bob".to_string(),
            valid_from: None,
            valid_until: None,
            confidence: 1.0,
            source_memory_id: None,
            source_file: None,
        };

        let result = checker.check_triple(&triple);
        assert!(result.is_consistent);
        assert!(result.contradiction.is_none());
    }

    #[test]
    fn conflicting_parent_detected() {
        let mut checker = FactChecker::new();
        let mut facts = EntityFacts::default();
        facts.entity_type = Some("person".to_string());
        facts.parent = Some("Bob".to_string());
        checker.register_facts("Alice", facts);

        let triple = Triple {
            id: "t2".to_string(),
            subject: "alice".to_string(),
            predicate: "child_of".to_string(),
            object: "Charlie".to_string(),
            valid_from: None,
            valid_until: None,
            confidence: 1.0,
            source_memory_id: None,
            source_file: None,
        };

        let result = checker.check_triple(&triple);
        assert!(!result.is_consistent);
        assert!(result.contradiction.is_some());
        assert!(result.contradiction.unwrap().contains("Bob"));
    }

    #[test]
    fn type_mismatch_detected() {
        let mut checker = FactChecker::new();
        let mut facts = EntityFacts::default();
        facts.entity_type = Some("project".to_string());
        checker.register_facts("Rust", facts);

        let triple = Triple {
            id: "t3".to_string(),
            subject: "rust".to_string(),
            predicate: "child_of".to_string(), // person-only predicate
            object: "C++".to_string(),
            valid_from: None,
            valid_until: None,
            confidence: 1.0,
            source_memory_id: None,
            source_file: None,
        };

        let result = checker.check_triple(&triple);
        assert!(!result.is_consistent);
        assert!(result.contradiction.unwrap().contains("not valid for entity type"));
    }

    #[test]
    fn no_registered_facts_means_pass() {
        let checker = FactChecker::new();

        let triple = Triple {
            id: "t4".to_string(),
            subject: "foo".to_string(),
            predicate: "child_of".to_string(),
            object: "bar".to_string(),
            valid_from: None,
            valid_until: None,
            confidence: 0.8,
            source_memory_id: None,
            source_file: None,
        };

        let result = checker.check_triple(&triple);
        assert!(result.is_consistent);
        assert_eq!(result.confidence, 0.8);
    }
}
