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

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::resolution::{ConflictInfo, ConflictResolver, CorrectionReport, Verdict};
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
    "child_of",
    "parent_of",
    "partner_of",
    "sibling_of",
    "born_on",
    "birthday",
    "works_for",
    "manages",
];

/// Predicates valid for organizations.
const ORG_PREDICATES: &[&str] = &["located_in", "subsidiary_of", "owns", "employs"];

/// Predicates valid for projects.
const PROJECT_PREDICATES: &[&str] = &["uses", "depends_on", "belongs_to", "has_member"];

/// Predicates valid for any entity type.
const UNIVERSAL_PREDICATES: &[&str] = &[
    "related_to",
    "has_property",
    "contains",
    "part_of",
    "located_in",
    "known_as",
    "also_called",
];

// ─── FactChecker ───────────────────────────────────────────────────────────────

/// Validates knowledge-graph triples against registered entity facts.
#[derive(Debug, Clone)]
pub struct FactChecker {
    /// Registered entity facts keyed by entity name (lowercased).
    entity_facts: HashMap<String, EntityFacts>,
    /// Stored triples for cross-agent conflict detection and consensus.
    triples: Vec<Triple>,
    /// Per-agent reliability weights. Default: Orchestrator=1.0, Reviewer=0.8, Executor=0.6, unknown=0.4.
    pub agent_weights: HashMap<String, f32>,
    /// Count of successful fact writes (used for weight adjustment logic).
    pub success_count: u32,
}

impl FactChecker {
    /// Create a new fact checker with no registered facts.
    pub fn new() -> Self {
        let mut agent_weights = HashMap::new();
        agent_weights.insert("Orchestrator".to_string(), 1.0);
        agent_weights.insert("Reviewer".to_string(), 0.8);
        agent_weights.insert("Executor".to_string(), 0.6);
        agent_weights.insert("unknown".to_string(), 0.4);
        Self {
            entity_facts: HashMap::new(),
            triples: Vec::new(),
            agent_weights,
            success_count: 0,
        }
    }

    /// Register known facts about an entity.
    pub fn register_facts(&mut self, entity: &str, facts: EntityFacts) {
        self.entity_facts.insert(entity.to_lowercase(), facts);
    }

    /// Store a triple for cross-agent conflict detection and consensus.
    pub fn register_triple(&mut self, triple: Triple) {
        self.success_count = self.success_count.wrapping_add(1);
        self.triples.push(triple);
    }

    /// Adjust an agent's reliability weight by `delta` (±0.05 per event).
    pub fn adjust_agent_weight(&mut self, agent: &str, delta: f32) {
        let key = if agent.is_empty() { "unknown" } else { agent };
        let entry = self.agent_weights.entry(key.to_string()).or_insert(0.4);
        *entry = (*entry + delta).clamp(0.1, 2.0);
    }

    /// Detect cross-agent conflict: same subject+predicate, different object, different source_agent.
    ///
    /// Returns the conflicting triple and a conflict score if found.
    /// Score = 0.4*confidence + 0.3*recency + 0.3*agent_weight.
    pub fn detect_conflict(&self, triple: &Triple) -> Option<(&Triple, f32)> {
        let agent = triple.source_agent.as_deref().unwrap_or("unknown");
        let weight = self.agent_weights.get(agent).copied().unwrap_or(0.4);
        let now = chrono::Utc::now();

        for existing in self.triples.iter().rev() {
            if existing.subject == triple.subject
                && existing.predicate == triple.predicate
                && existing.object != triple.object
                && existing.source_agent != triple.source_agent
                && existing.valid_until.is_none()
            {
                let age_hours = existing
                    .valid_from
                    .map(|t| (now - t).num_hours().max(0) as f32)
                    .unwrap_or(24.0);
                let recency_factor = (1.0 / (1.0 + age_hours)).clamp(0.0, 1.0);
                let score = 0.4 * existing.confidence + 0.3 * recency_factor + 0.3 * weight;
                return Some((existing, score));
            }
        }
        None
    }

    /// Check if 3+ distinct agents agree on the same (subject, predicate, object).
    pub fn detect_consensus(&self, subject: &str, predicate: &str, object: &str) -> bool {
        let agents: std::collections::HashSet<&str> = self
            .triples
            .iter()
            .filter(|t| {
                t.subject == subject
                    && t.predicate == predicate
                    && t.object == object
                    && t.valid_until.is_none()
            })
            .filter_map(|t| t.source_agent.as_deref())
            .collect();
        agents.len() >= 3
    }

    /// Get the consensus confidence for a fact (0.95 if 3+ agents agree, otherwise original).
    pub fn consensus_confidence(&self, triple: &Triple) -> f32 {
        if self.detect_consensus(&triple.subject, &triple.predicate, &triple.object) {
            0.95
        } else {
            triple.confidence
        }
    }

    /// Count how many distinct agents have registered the same (subject, predicate, object).
    fn count_agents_for(&self, subject: &str, predicate: &str, object: &str) -> usize {
        self.triples
            .iter()
            .filter(|t| {
                t.subject == subject
                    && t.predicate == predicate
                    && t.object == object
                    && t.valid_until.is_none()
            })
            .filter_map(|t| t.source_agent.as_deref())
            .collect::<HashSet<_>>()
            .len()
    }

    /// Auto-correct: scan all stored triples for conflicts, resolve each via
    /// [`ConflictResolver`], and apply verdicts (invalidate old entries for
    /// `ReplaceWithNew`, boost confidence for `PromoteConsensus`, prune for
    /// `KeepExisting`, log for `FlagForReview`).
    ///
    /// Returns a [`CorrectionReport`] summarising what was done.
    pub fn auto_correct(&mut self) -> CorrectionReport {
        let resolver = ConflictResolver::default();
        let mut corrected = 0usize;
        let mut pruned = 0usize;
        let mut flagged = 0usize;

        let mut conflict_infos: Vec<ConflictInfo> = Vec::new();
        let mut processed_pairs: HashSet<(usize, usize)> = HashSet::new();

        let n = self.triples.len();
        for i in 0..n {
            let t_a = &self.triples[i];
            if t_a.valid_until.is_some() {
                continue;
            }
            for j in 0..n {
                if i == j {
                    continue;
                }
                let pair = (i.min(j), i.max(j));
                if processed_pairs.contains(&pair) {
                    continue;
                }
                let t_b = &self.triples[j];
                if t_b.subject == t_a.subject
                    && t_b.predicate == t_a.predicate
                    && t_b.object != t_a.object
                    && t_b.source_agent != t_a.source_agent
                    && t_b.valid_until.is_none()
                {
                    processed_pairs.insert(pair);

                    let consensus_count =
                        self.count_agents_for(&t_a.subject, &t_a.predicate, &t_a.object);

                    conflict_infos.push(ConflictInfo {
                        existing_id: t_b.id.clone(),
                        new_id: t_a.id.clone(),
                        subject: t_a.subject.clone(),
                        predicate: t_a.predicate.clone(),
                        existing_object: t_b.object.clone(),
                        new_object: t_a.object.clone(),
                        existing_confidence: t_b.confidence,
                        new_confidence: t_a.confidence,
                        new_agent: t_a
                            .source_agent
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                        consensus_count,
                        conflict_score: 0.5,
                    });
                }
            }
        }

        'conflict_loop: for info in &conflict_infos {
            match resolver.resolve(info) {
                Verdict::ReplaceWithNew(_) => {
                    for t in self.triples.iter_mut() {
                        if t.id == info.existing_id {
                            t.valid_until = Some(Utc::now());
                            continue 'conflict_loop;
                        }
                    }
                    corrected += 1;
                    pruned += 1;
                }
                Verdict::PromoteConsensus(ref id) => {
                    for t in self.triples.iter_mut() {
                        if t.id == *id {
                            t.confidence = 0.95;
                            break;
                        }
                    }
                    corrected += 1;
                }
                Verdict::KeepExisting(_) => {
                    for t in self.triples.iter_mut() {
                        if t.id == info.new_id {
                            t.valid_until = Some(Utc::now());
                            break;
                        }
                    }
                    corrected += 1;
                    pruned += 1;
                }
                Verdict::FlagForReview(_) => {
                    flagged += 1;
                }
            }
        }

        CorrectionReport {
            corrected,
            pruned,
            flagged,
        }
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
                    if obj_lc != parent.to_lowercase() && !obj_lc.is_empty() && !parent.is_empty() {
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
                    if obj_lc != partner.to_lowercase() && !obj_lc.is_empty() && !partner.is_empty()
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
            Some(format!(
                "Review and reconcile the conflicting facts for {}",
                triple.subject
            ))
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
        graph
            .all_triples()
            .iter()
            .map(|t| self.check_triple(t))
            .collect()
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
            source_agent: None,
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
            source_agent: None,
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
            source_agent: None,
        };

        let result = checker.check_triple(&triple);
        assert!(!result.is_consistent);
        assert!(result
            .contradiction
            .unwrap()
            .contains("not valid for entity type"));
    }

    // ── auto_correct tests ──────────────────────────────────────────

    fn make_triple(
        id: &str,
        subject: &str,
        predicate: &str,
        object: &str,
        confidence: f32,
        agent: &str,
    ) -> Triple {
        Triple {
            id: id.to_string(),
            subject: subject.to_string(),
            predicate: predicate.to_string(),
            object: object.to_string(),
            valid_from: None,
            valid_until: None,
            confidence,
            source_memory_id: None,
            source_file: None,
            source_agent: Some(agent.to_string()),
        }
    }

    #[test]
    fn test_auto_correct_replace() {
        let mut checker = FactChecker::new();
        checker.register_triple(make_triple(
            "t1",
            "Alice",
            "partner_of",
            "Bob",
            0.3,
            "unknown",
        ));
        checker.register_triple(make_triple(
            "t2",
            "Alice",
            "partner_of",
            "Charlie",
            0.9,
            "Orchestrator",
        ));

        let report = checker.auto_correct();
        assert!(report.corrected >= 1, "should have at least one correction");
        assert!(report.pruned >= 1, "existing triple should be pruned");

        let t1 = checker.triples.iter().find(|t| t.id == "t1").unwrap();
        assert!(
            t1.valid_until.is_some(),
            "existing triple should be invalidated"
        );
    }

    #[test]
    fn test_auto_correct_consensus() {
        let mut checker = FactChecker::new();
        checker.register_triple(make_triple(
            "t1",
            "Bob",
            "works_for",
            "Acme",
            0.7,
            "Orchestrator",
        ));
        checker.register_triple(make_triple(
            "t2",
            "Bob",
            "works_for",
            "Acme",
            0.7,
            "Reviewer",
        ));
        checker.register_triple(make_triple(
            "t3",
            "Bob",
            "works_for",
            "Acme",
            0.7,
            "Executor",
        ));
        checker.register_triple(make_triple(
            "t4",
            "Bob",
            "works_for",
            "Globex",
            0.7,
            "unknown",
        ));

        let report = checker.auto_correct();

        let consensus_triple = checker
            .triples
            .iter()
            .find(|t| t.subject == "Bob" && t.object == "Acme" && t.confidence >= 0.95);
        assert!(
            consensus_triple.is_some(),
            "consensus triple should have confidence boosted to 0.95"
        );
        assert!(report.corrected > 0, "should have corrections");
    }

    #[test]
    fn test_auto_correct_noop() {
        let mut checker = FactChecker::new();
        checker.register_triple(make_triple(
            "t1",
            "Carol",
            "child_of",
            "Dave",
            0.8,
            "Orchestrator",
        ));
        checker.register_triple(make_triple(
            "t2",
            "Eve",
            "works_for",
            "Inc",
            0.7,
            "Reviewer",
        ));

        let report = checker.auto_correct();
        assert_eq!(report.corrected, 0, "no conflicts → nothing corrected");
        assert_eq!(report.pruned, 0, "no conflicts → nothing pruned");
        assert_eq!(report.flagged, 0, "no conflicts → nothing flagged");
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
            source_agent: None,
        };

        let result = checker.check_triple(&triple);
        assert!(result.is_consistent);
        assert_eq!(result.confidence, 0.8);
    }
}
