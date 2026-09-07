use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::store::Result as StoreResult;

/// Disambiguation key to distinguish entities with the same name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DisambiguationKey {
    Id(String),
    DobContext(String),
    ProjectScope(String),
}

impl DisambiguationKey {
    /// Convert the key to a storable string representation.
    pub fn to_key_str(&self) -> String {
        match self {
            DisambiguationKey::Id(s) => format!("id:{s}"),
            DisambiguationKey::DobContext(s) => format!("dob:{s}"),
            DisambiguationKey::ProjectScope(s) => format!("project:{s}"),
        }
    }
}

impl std::fmt::Display for DisambiguationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisambiguationKey::Id(s) => write!(f, "id={s}"),
            DisambiguationKey::DobContext(s) => write!(f, "dob={s}"),
            DisambiguationKey::ProjectScope(s) => write!(f, "project={s}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntityRecord {
    pub name: String,
    pub key: DisambiguationKey,
    pub confidence: f32,
    pub occurrences: usize,
}

/// A single evolution event in an entity's timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionRecord {
    pub id: i64,
    pub entity_name: String,
    pub entity_key: String,
    pub agent_id: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
    pub confidence: Option<f32>,
    pub operation: String,
    pub recorded_at_ms: i64,
}

impl EvolutionRecord {
    /// Format the record as a human-readable sentence.
    pub fn to_sentence(&self) -> String {
        let ts = self.recorded_at_ms;
        let conf_str = self
            .confidence
            .map(|c| format!(" (confidence: {c:.2})"))
            .unwrap_or_default();
        match self.operation.as_str() {
            "register" => {
                format!(
                    "Entity '{}' (key: {}) was registered by agent '{}' at t={}{}",
                    self.entity_name, self.entity_key, self.agent_id, ts, conf_str
                )
            }
            "update" => {
                let old = self.old_value.as_deref().unwrap_or("(none)");
                let new = self.new_value.as_deref().unwrap_or("(none)");
                format!(
                    "Entity '{}' was updated by agent '{}' at t={}: '{}' → '{}'{}",
                    self.entity_name, self.agent_id, ts, old, new, conf_str
                )
            }
            "resolve" => {
                let resolved = self.new_value.as_deref().unwrap_or("(none)");
                format!(
                    "Entity '{}' was resolved by agent '{}' at t={} → '{}'{}",
                    self.entity_name, self.agent_id, ts, resolved, conf_str
                )
            }
            other => {
                format!(
                    "Entity '{}' had operation '{}' by agent '{}' at t={}{}",
                    self.entity_name, other, self.agent_id, ts, conf_str
                )
            }
        }
    }
}

pub struct EntityRegistry {
    entities: HashMap<String, Vec<EntityRecord>>,
    evolution_tracking: bool,
    evolutions: Vec<EvolutionRecord>,
}

impl Default for EntityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EntityRegistry {
    pub fn new() -> Self {
        Self {
            entities: HashMap::new(),
            evolution_tracking: false,
            evolutions: Vec::new(),
        }
    }

    /// Enable process-local evolution tracking.
    ///
    /// Durable knowledge belongs to the canonical `MemoryStore`; this registry
    /// deliberately remains a derived, rebuildable context index.
    pub fn with_evolution_tracking(mut self) -> Self {
        self.evolution_tracking = true;
        self
    }

    /// Check whether evolution context collection is enabled.
    pub fn has_store(&self) -> bool {
        self.evolution_tracking
    }

    pub fn register(&mut self, name: &str, key: DisambiguationKey, confidence: f32) {
        let record = EntityRecord {
            name: name.to_string(),
            key,
            confidence,
            occurrences: 1,
        };
        self.entities
            .entry(name.to_string())
            .or_default()
            .push(record);
    }

    /// Register an entity and, when enabled, record a rebuildable evolution event.
    pub fn register_persistent(
        &mut self,
        name: &str,
        key: DisambiguationKey,
        confidence: f32,
        agent_id: &str,
    ) -> StoreResult<()> {
        let key_str = key.to_key_str();
        // Track the old value for update detection
        let previous = self.entities.get(name);
        let (operation, old_val) = if previous.is_some() {
            ("update", Some(format!("{}", key)))
        } else {
            ("register", None::<String>)
        };

        // In-memory register (same as register())
        let record = EntityRecord {
            name: name.to_string(),
            key: key.clone(),
            confidence,
            occurrences: 1,
        };
        self.entities
            .entry(name.to_string())
            .or_default()
            .push(record);

        if self.evolution_tracking {
            self.evolutions.push(EvolutionRecord {
                id: self.evolutions.len() as i64 + 1,
                entity_name: name.to_string(),
                entity_key: key_str,
                agent_id: agent_id.to_string(),
                old_value: old_val,
                new_value: Some(format!("{key}")),
                confidence: Some(confidence),
                operation: operation.to_string(),
                recorded_at_ms: chrono::Utc::now().timestamp_millis(),
            });
        }

        Ok(())
    }

    /// Register an entity update where we know the old and new values explicitly.
    pub fn register_persistent_with_values(
        &mut self,
        name: &str,
        key: DisambiguationKey,
        confidence: f32,
        agent_id: &str,
        old_value: Option<&str>,
        new_value: Option<&str>,
        operation: &str,
    ) -> StoreResult<()> {
        let key_str = key.to_key_str();

        let record = EntityRecord {
            name: name.to_string(),
            key: key.clone(),
            confidence,
            occurrences: 1,
        };
        self.entities
            .entry(name.to_string())
            .or_default()
            .push(record);

        if self.evolution_tracking {
            self.evolutions.push(EvolutionRecord {
                id: self.evolutions.len() as i64 + 1,
                entity_name: name.to_string(),
                entity_key: key_str,
                agent_id: agent_id.to_string(),
                old_value: old_value.map(str::to_string),
                new_value: new_value.map(str::to_string),
                confidence: Some(confidence),
                operation: operation.to_string(),
                recorded_at_ms: chrono::Utc::now().timestamp_millis(),
            });
        }

        Ok(())
    }

    /// Resolve a name to its most confident disambiguated entity.
    pub fn resolve(&self, name: &str, context: Option<&str>) -> Option<&EntityRecord> {
        let entries = self.entities.get(name)?;
        if entries.len() == 1 {
            return entries.first();
        }
        if let Some(ctx) = context {
            for e in entries {
                match &e.key {
                    DisambiguationKey::ProjectScope(scope) if ctx.contains(scope) => {
                        return Some(e)
                    }
                    DisambiguationKey::DobContext(dob) if ctx.contains(dob) => return Some(e),
                    DisambiguationKey::Id(id) if ctx.contains(id) => return Some(e),
                    _ => {}
                }
            }
        }
        entries
            .iter()
            .max_by(|a, b| a.confidence.total_cmp(&b.confidence))
    }

    /// Retrieve the chronological evolution timeline for an entity.
    ///
    /// Returns an empty `Vec` if tracking is disabled or no records exist.
    pub fn get_entity_timeline(&self, entity_name: &str) -> StoreResult<Vec<EvolutionRecord>> {
        Ok(self
            .evolutions
            .iter()
            .filter(|record| record.entity_name == entity_name)
            .cloned()
            .collect())
    }

    /// Build a human-readable narrative arc of how an entity changed across agents.
    ///
    /// Returns a multi-line story string. Returns an informational message if
    /// no store is attached or no timeline exists.
    pub fn get_narrative_arc(&self, entity_name: &str) -> StoreResult<String> {
        let timeline = self.get_entity_timeline(entity_name)?;
        if timeline.is_empty() {
            return Ok(format!(
                "No evolution history recorded for entity '{entity_name}'."
            ));
        }

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!(
            "=== Narrative Arc for '{}' ({} events) ===",
            entity_name,
            timeline.len()
        ));

        // Group by agent for a summary
        let mut agents: HashMap<String, usize> = HashMap::new();
        for ev in &timeline {
            *agents.entry(ev.agent_id.clone()).or_insert(0) += 1;
        }

        lines.push("Agents involved:".to_string());
        for (agent, count) in &agents {
            lines.push(format!("  - {agent}: {count} event(s)"));
        }

        lines.push("Chronology:".to_string());
        for ev in &timeline {
            lines.push(format!("  {}", ev.to_sentence()));
        }

        Ok(lines.join("\n"))
    }

    /// Get the most recent N entity evolution events across all entities.
    ///
    /// Returns an empty `Vec` if tracking is disabled.
    pub fn get_recent_evolutions(&self, limit: usize) -> StoreResult<Vec<EvolutionRecord>> {
        let start = self.evolutions.len().saturating_sub(limit);
        Ok(self.evolutions[start..].to_vec())
    }

    pub fn count(&self) -> usize {
        self.entities.len()
    }
    pub fn entries_for(&self, name: &str) -> usize {
        self.entities.get(name).map(|v| v.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t04_same_name_different_id() {
        let mut r = EntityRegistry::new();
        r.register("张三", DisambiguationKey::Id("id-001".into()), 0.9);
        r.register("张三", DisambiguationKey::Id("id-002".into()), 0.8);
        assert_eq!(r.entries_for("张三"), 2);
        let e = r.resolve("张三", Some("id-001"));
        assert!(e.is_some());
        assert_eq!(e.unwrap().key, DisambiguationKey::Id("id-001".into()));
    }

    #[test]
    fn t04_dob_disambiguation() {
        let mut r = EntityRegistry::new();
        r.register("张三", DisambiguationKey::DobContext("1985".into()), 0.9);
        r.register("张三", DisambiguationKey::DobContext("1990".into()), 0.9);
        let e85 = r.resolve("张三", Some("生于1985年"));
        let e90 = r.resolve("张三", Some("1990年出生"));
        assert_eq!(
            e85.unwrap().key,
            DisambiguationKey::DobContext("1985".into())
        );
        assert_eq!(
            e90.unwrap().key,
            DisambiguationKey::DobContext("1990".into())
        );
    }

    #[test]
    fn t04_project_disambiguation() {
        let mut r = EntityRegistry::new();
        r.register("张三", DisambiguationKey::ProjectScope("cowd".into()), 0.9);
        r.register(
            "李四",
            DisambiguationKey::ProjectScope("hermes".into()),
            0.8,
        );
        let e = r.resolve("张三", Some("working on cowd project"));
        assert!(e.is_some());
        assert_eq!(
            e.unwrap().key,
            DisambiguationKey::ProjectScope("cowd".into())
        );
    }

    #[test]
    fn test_register_persistent_without_store() {
        let mut r = EntityRegistry::new();
        assert!(!r.has_store());
        let result = r.register_persistent(
            "test_entity",
            DisambiguationKey::Id("e-1".into()),
            0.9,
            "Orchestrator",
        );
        assert!(result.is_ok());
        assert_eq!(r.entries_for("test_entity"), 1);
    }

    #[test]
    fn test_register_persistent_with_store() {
        let mut r = EntityRegistry::new().with_evolution_tracking();
        assert!(r.has_store());

        let result = r.register_persistent(
            "alice",
            DisambiguationKey::Id("alice-1".into()),
            0.95,
            "Orchestrator",
        );
        assert!(result.is_ok());
        assert_eq!(r.entries_for("alice"), 1);

        // Update the same entity
        let result2 = r.register_persistent(
            "alice",
            DisambiguationKey::Id("alice-2".into()),
            0.8,
            "Executor",
        );
        assert!(result2.is_ok());
        assert_eq!(r.entries_for("alice"), 2);

        let timeline = r.get_entity_timeline("alice").expect("get timeline");
        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].operation, "register");
        assert_eq!(timeline[0].agent_id, "Orchestrator");
        assert_eq!(timeline[1].operation, "update");
        assert_eq!(timeline[1].agent_id, "Executor");
    }

    #[test]
    fn test_narrative_arc() {
        let mut r = EntityRegistry::new().with_evolution_tracking();

        r.register_persistent(
            "bob",
            DisambiguationKey::ProjectScope("cowd".into()),
            0.9,
            "Orchestrator",
        )
        .unwrap();
        r.register_persistent(
            "bob",
            DisambiguationKey::ProjectScope("hermes".into()),
            0.7,
            "Executor",
        )
        .unwrap();

        let arc = r.get_narrative_arc("bob").expect("narrative arc");
        assert!(arc.contains("bob"));
        assert!(arc.contains("Orchestrator"));
        assert!(arc.contains("Executor"));
        assert!(arc.contains("register"));
        assert!(arc.contains("update"));
    }

    #[test]
    fn test_narrative_arc_empty() {
        let r = EntityRegistry::new();
        let arc = r.get_narrative_arc("nonexistent").expect("narrative arc");
        assert!(arc.contains("No evolution history"));
    }

    #[test]
    fn test_timeline_empty_without_store() {
        let r = EntityRegistry::new();
        let timeline = r.get_entity_timeline("anything").expect("timeline");
        assert!(timeline.is_empty());
    }

    #[test]
    fn test_recent_evolutions() {
        let mut r = EntityRegistry::new().with_evolution_tracking();

        r.register_persistent("e1", DisambiguationKey::Id("e1".into()), 0.9, "AgentA")
            .unwrap();
        r.register_persistent("e2", DisambiguationKey::Id("e2".into()), 0.8, "AgentB")
            .unwrap();

        let recent = r.get_recent_evolutions(10).expect("recent evolutions");
        assert_eq!(recent.len(), 2);
    }

    #[test]
    fn test_disambiguation_key_display() {
        let key = DisambiguationKey::Id("test-123".into());
        assert_eq!(key.to_key_str(), "id:test-123");
        assert_eq!(format!("{key}"), "id=test-123");

        let key = DisambiguationKey::DobContext("1990".into());
        assert_eq!(key.to_key_str(), "dob:1990");

        let key = DisambiguationKey::ProjectScope("cowd".into());
        assert_eq!(key.to_key_str(), "project:cowd");
    }

    #[test]
    fn test_evolution_record_sentence() {
        let rec = EvolutionRecord {
            id: 1,
            entity_name: "test".into(),
            entity_key: "id:1".into(),
            agent_id: "Orchestrator".into(),
            old_value: None,
            new_value: Some("id=1".into()),
            confidence: Some(0.95),
            operation: "register".into(),
            recorded_at_ms: 1000,
        };
        let s = rec.to_sentence();
        assert!(s.contains("registered"));
        assert!(s.contains("Orchestrator"));
        assert!(s.contains("0.95"));

        let rec2 = EvolutionRecord {
            id: 2,
            entity_name: "test".into(),
            entity_key: "id:2".into(),
            agent_id: "Executor".into(),
            old_value: Some("id=1".into()),
            new_value: Some("id=2".into()),
            confidence: Some(0.8),
            operation: "update".into(),
            recorded_at_ms: 2000,
        };
        let s2 = rec2.to_sentence();
        assert!(s2.contains("updated"));
        assert!(s2.contains("Executor"));
        assert!(s2.contains("id=1"));
        assert!(s2.contains("id=2"));
    }
}
