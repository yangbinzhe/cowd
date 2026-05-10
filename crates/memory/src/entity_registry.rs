use std::collections::HashMap;

/// Disambiguation key to distinguish entities with the same name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DisambiguationKey {
    Id(String),
    DobContext(String),
    ProjectScope(String),
}

#[derive(Debug, Clone)]
pub struct EntityRecord {
    pub name: String,
    pub key: DisambiguationKey,
    pub confidence: f32,
    pub occurrences: usize,
}

pub struct EntityRegistry {
    entities: HashMap<String, Vec<EntityRecord>>,
}

impl EntityRegistry {
    pub fn new() -> Self { Self { entities: HashMap::new() } }

    pub fn register(&mut self, name: &str, key: DisambiguationKey, confidence: f32) {
        let record = EntityRecord { name: name.to_string(), key, confidence, occurrences: 1 };
        self.entities.entry(name.to_string()).or_default().push(record);
    }

    /// Resolve a name to its most confident disambiguated entity.
    pub fn resolve(&self, name: &str, context: Option<&str>) -> Option<&EntityRecord> {
        let entries = self.entities.get(name)?;
        if entries.len() == 1 { return entries.first(); }
        // Try to match by context
        if let Some(ctx) = context {
            for e in entries {
                match &e.key {
                    DisambiguationKey::ProjectScope(scope) if ctx.contains(scope) => return Some(e),
                    DisambiguationKey::DobContext(dob) if ctx.contains(dob) => return Some(e),
                    DisambiguationKey::Id(id) if ctx.contains(id) => return Some(e),
                    _ => {}
                }
            }
        }
        // Fallback: highest confidence
        entries.iter().max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap())
    }

    pub fn count(&self) -> usize { self.entities.len() }
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
        // Context matching should distinguish them
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
        assert_eq!(e85.unwrap().key, DisambiguationKey::DobContext("1985".into()));
        assert_eq!(e90.unwrap().key, DisambiguationKey::DobContext("1990".into()));
    }

    #[test]
    fn t04_project_disambiguation() {
        let mut r = EntityRegistry::new();
        r.register("张三", DisambiguationKey::ProjectScope("cowd".into()), 0.9);
        r.register("李四", DisambiguationKey::ProjectScope("hermes".into()), 0.8);
        let e = r.resolve("张三", Some("working on cowd project"));
        assert!(e.is_some());
        assert_eq!(e.unwrap().key, DisambiguationKey::ProjectScope("cowd".into()));
    }
}