//! Seed system and decision threads.
//!
//! **Seeds** are pre-authored context fragments injected into the prompt when a
//! trigger condition fires (phase transition, keyword match, scheduled time, or
//! manual activation).
//!
//! **Decision threads** provide a persistent audit trail of choices made across
//! sessions, grouping related `DecisionEntry` records by a shared topic string.
//!
//! Both registries hold their state in-memory for the lifetime of the process.
//! Persistence can be layered on top by the orchestrator if needed.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    error::MemoryError,
    types::{
        DecisionEntry, DecisionStatus, DecisionThread, Priority, Seed, SeedId, SeedTrigger,
    },
};

/// Result alias used throughout this module.
pub type Result<T> = std::result::Result<T, MemoryError>;

// ─── SeedRegistry ─────────────────────────────────────────────────────────────

/// In-memory registry that stores seeds and evaluates trigger conditions.
///
/// Seeds are matched against the current phase and keyword set on every call to
/// [`check_triggers`](Self::check_triggers).  Once a seed has surfaced it is
/// marked inactive so it does not fire again unless explicitly re-activated.
pub struct SeedRegistry {
    seeds: Vec<Seed>,
}

impl SeedRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self { seeds: Vec::new() }
    }

    // ─── Mutation ────────────────────────────────────────────────────────────

    /// Plant a new seed and return its ID.
    pub fn plant(
        &mut self,
        title: &str,
        content: &str,
        trigger: SeedTrigger,
        priority: Priority,
    ) -> SeedId {
        let id = Uuid::new_v4();
        self.seeds.push(Seed {
            id,
            name: title.to_owned(),
            content: content.to_owned(),
            trigger,
            priority,
            active: true,
            created_at: Utc::now(),
        });
        id
    }

    /// Register a pre-built seed (lower-level API kept for compatibility).
    pub fn register(&mut self, seed: Seed) {
        self.seeds.push(seed);
    }

    /// Remove a seed by ID.
    pub fn deregister(&mut self, id: &SeedId) {
        self.seeds.retain(|s| &s.id != id);
    }

    /// Mark a seed as surfaced (sets `active = false`).
    ///
    /// Returns `Err(NotFound)` if no seed with that ID exists.
    pub fn mark_surfaced(&mut self, id: &SeedId) -> Result<()> {
        self.seeds
            .iter_mut()
            .find(|s| &s.id == id)
            .ok_or_else(|| MemoryError::NotFound(id.to_string()))
            .map(|s| s.active = false)
    }

    /// Remove a seed permanently.
    ///
    /// Returns `Err(NotFound)` if no seed with that ID exists.
    pub fn remove(&mut self, id: &SeedId) -> Result<()> {
        let len_before = self.seeds.len();
        self.seeds.retain(|s| &s.id != id);
        if self.seeds.len() == len_before {
            return Err(MemoryError::NotFound(id.to_string()));
        }
        Ok(())
    }

    // ─── Queries ─────────────────────────────────────────────────────────────

    /// Return all active seeds whose trigger fires for `phase` / `keywords` /
    /// `now`.
    ///
    /// Matched seeds are automatically marked as surfaced (deactivated).
    pub fn check_triggers(
        &mut self,
        current_phase: &str,
        keywords: &[String],
        now: DateTime<Utc>,
    ) -> Vec<Seed> {
        let kw_refs: Vec<&str> = keywords.iter().map(std::string::String::as_str).collect();
        let mut matched = Vec::new();

        for seed in &mut self.seeds {
            if seed.active && trigger_fires(&seed.trigger, current_phase, &kw_refs, now) {
                seed.active = false;
                matched.push(seed.clone());
            }
        }
        matched
    }

    /// Return matching seeds *without* mutating state (read-only).
    #[must_use]
    pub fn matching_seeds<'a>(&'a self, phase: &str, keywords: &[&str]) -> Vec<&'a Seed> {
        self.seeds
            .iter()
            .filter(|s| s.active && trigger_fires(&s.trigger, phase, keywords, Utc::now()))
            .collect()
    }

    /// List all seeds that have not yet surfaced.
    #[must_use]
    pub fn list_pending(&self) -> Vec<&Seed> {
        self.seeds.iter().filter(|s| s.active).collect()
    }

    /// Return the total number of seeds (active + inactive).
    #[must_use]
    pub fn len(&self) -> usize {
        self.seeds.len()
    }

    /// Return `true` if the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seeds.is_empty()
    }
}

impl Default for SeedRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── DecisionThreadStore ──────────────────────────────────────────────────────

/// Persistent (in-process) log of decisions made across sessions, organised
/// into per-topic threads.
pub struct DecisionThreadStore {
    threads: Vec<DecisionThread>,
}

impl DecisionThreadStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            threads: Vec::new(),
        }
    }

    // ─── Thread management ───────────────────────────────────────────────────

    /// Create a new thread for `topic` and return its ID.
    ///
    /// If a thread for `topic` already exists, its ID is returned unchanged.
    pub fn create_thread(&mut self, topic: &str) -> String {
        if let Some(existing) = self.threads.iter().find(|t| t.topic == topic) {
            return existing.id.clone();
        }
        let id = Uuid::new_v4().to_string();
        self.threads.push(DecisionThread {
            id: id.clone(),
            topic: topic.to_owned(),
            entries: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        id
    }

    /// Open (or create) the thread for `topic` and return a mutable reference.
    pub fn get_or_create(&mut self, topic: &str) -> &mut DecisionThread {
        if !self.threads.iter().any(|t| t.topic == topic) {
            self.threads.push(DecisionThread {
                id: Uuid::new_v4().to_string(),
                topic: topic.to_owned(),
                entries: Vec::new(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            });
        }
        self.threads
            .iter_mut()
            .find(|t| t.topic == topic)
            .expect("just inserted")
    }

    // ─── Entry management ────────────────────────────────────────────────────

    /// Append a new decision entry to the thread identified by `topic`.
    ///
    /// Creates the thread if it does not yet exist.
    pub fn append_entry(
        &mut self,
        topic: &str,
        phase: &str,
        decision: &str,
        rationale: &str,
        status: DecisionStatus,
    ) {
        let thread = self.get_or_create(topic);
        let entry = DecisionEntry {
            id: Uuid::new_v4().to_string(),
            summary: format!("[{phase}] {decision}"),
            rationale: rationale.to_owned(),
            status,
            alternatives: Vec::new(),
            made_at: Utc::now(),
        };
        thread.entries.push(entry);
        thread.updated_at = Utc::now();
    }

    /// Record a decision using the legacy signature (kept for compatibility).
    pub fn record(
        &mut self,
        topic: &str,
        summary: String,
        rationale: String,
        alternatives: Vec<String>,
    ) {
        let thread = self.get_or_create(topic);
        let entry = DecisionEntry {
            id: Uuid::new_v4().to_string(),
            summary,
            rationale,
            status: DecisionStatus::Implemented,
            alternatives,
            made_at: Utc::now(),
        };
        thread.entries.push(entry);
        thread.updated_at = Utc::now();
    }

    // ─── Queries ─────────────────────────────────────────────────────────────

    /// Return the complete decision thread for `topic`, or `None` if not found.
    #[must_use]
    pub fn get_thread(&self, topic: &str) -> Option<&DecisionThread> {
        self.threads.iter().find(|t| t.topic == topic)
    }

    /// List the topic strings of all threads.
    #[must_use]
    pub fn list_threads(&self) -> Vec<&str> {
        self.threads.iter().map(|t| t.topic.as_str()).collect()
    }

    /// Return all threads whose topic or any decision summary contains
    /// `keyword` (case-insensitive substring search).
    #[must_use]
    pub fn search_threads(&self, keyword: &str) -> Vec<&DecisionThread> {
        let kw = keyword.to_lowercase();
        self.threads
            .iter()
            .filter(|t| {
                t.topic.to_lowercase().contains(&kw)
                    || t.entries
                        .iter()
                        .any(|e| e.summary.to_lowercase().contains(&kw))
            })
            .collect()
    }

    /// Return the number of threads in the store.
    #[must_use]
    pub fn len(&self) -> usize {
        self.threads.len()
    }

    /// Return `true` if the store contains no threads.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.threads.is_empty()
    }
}

impl Default for DecisionThreadStore {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Free helpers ─────────────────────────────────────────────────────────────

fn trigger_fires(
    trigger: &SeedTrigger,
    phase: &str,
    keywords: &[&str],
    now: DateTime<Utc>,
) -> bool {
    match trigger {
        SeedTrigger::Phase(p) => p == phase,
        SeedTrigger::Keyword(kws) => kws.iter().any(|k| keywords.contains(&k.as_str())),
        SeedTrigger::Time(t) => now >= *t,
        SeedTrigger::Manual => false,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    // ── SeedRegistry tests ───────────────────────────────────────────────────

    #[test]
    fn plant_and_check_phase_trigger() {
        let mut reg = SeedRegistry::new();
        reg.plant("test", "content", SeedTrigger::Phase("build".into()), Priority::Normal);
        let hits = reg.check_triggers("build", &[], Utc::now());
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "test");
    }

    #[test]
    fn triggered_seed_is_deactivated() {
        let mut reg = SeedRegistry::new();
        reg.plant("once", "c", SeedTrigger::Phase("x".into()), Priority::Low);
        reg.check_triggers("x", &[], Utc::now());
        // Second call should not fire.
        let hits = reg.check_triggers("x", &[], Utc::now());
        assert!(hits.is_empty());
    }

    #[test]
    fn keyword_trigger() {
        let mut reg = SeedRegistry::new();
        reg.plant(
            "kwseed",
            "c",
            SeedTrigger::Keyword(vec!["rust".into(), "memory".into()]),
            Priority::High,
        );
        let kws: Vec<String> = vec!["memory".into()];
        let hits = reg.check_triggers("any", &kws, Utc::now());
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn time_trigger_past() {
        let mut reg = SeedRegistry::new();
        let past = Utc::now() - Duration::hours(1);
        reg.plant("timeseed", "c", SeedTrigger::Time(past), Priority::Normal);
        let hits = reg.check_triggers("any", &[], Utc::now());
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn time_trigger_future_does_not_fire() {
        let mut reg = SeedRegistry::new();
        let future = Utc::now() + Duration::hours(1);
        reg.plant("future", "c", SeedTrigger::Time(future), Priority::Normal);
        let hits = reg.check_triggers("any", &[], Utc::now());
        assert!(hits.is_empty());
    }

    #[test]
    fn list_pending_excludes_surfaced() {
        let mut reg = SeedRegistry::new();
        let id = reg.plant("s", "c", SeedTrigger::Phase("p".into()), Priority::Normal);
        assert_eq!(reg.list_pending().len(), 1);
        reg.mark_surfaced(&id).unwrap();
        assert_eq!(reg.list_pending().len(), 0);
    }

    #[test]
    fn remove_nonexistent_returns_err() {
        let mut reg = SeedRegistry::new();
        let fake = Uuid::new_v4();
        assert!(reg.remove(&fake).is_err());
    }

    // ── DecisionThreadStore tests ────────────────────────────────────────────

    #[test]
    fn create_thread_idempotent() {
        let mut store = DecisionThreadStore::new();
        let id1 = store.create_thread("architecture");
        let id2 = store.create_thread("architecture");
        assert_eq!(id1, id2);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn append_entry_and_get_thread() {
        let mut store = DecisionThreadStore::new();
        store.append_entry(
            "storage",
            "design",
            "Use SQLite",
            "Simple and portable",
            DecisionStatus::Implemented,
        );
        let thread = store.get_thread("storage").unwrap();
        assert_eq!(thread.entries.len(), 1);
        assert!(thread.entries[0].summary.contains("SQLite"));
    }

    #[test]
    fn search_threads_by_topic_substring() {
        let mut store = DecisionThreadStore::new();
        store.create_thread("rust memory model");
        store.create_thread("api design");
        let results = store.search_threads("memory");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].topic, "rust memory model");
    }

    #[test]
    fn search_threads_by_entry_content() {
        let mut store = DecisionThreadStore::new();
        store.append_entry(
            "infra",
            "ops",
            "deploy on k8s",
            "scalable",
            DecisionStatus::Implemented,
        );
        let results = store.search_threads("k8s");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn list_threads_returns_all_topics() {
        let mut store = DecisionThreadStore::new();
        store.create_thread("a");
        store.create_thread("b");
        store.create_thread("c");
        let topics = store.list_threads();
        assert_eq!(topics.len(), 3);
        assert!(topics.contains(&"a"));
        assert!(topics.contains(&"b"));
        assert!(topics.contains(&"c"));
    }
}
