use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

#[derive(Debug, Clone)]
pub struct SessionEvent {
    pub event_type: String,
    pub category: String,
    pub data_summary: String,
    pub priority: u8,
    pub data_hash: u64,
    pub timestamp: i64,
    pub project_dir: Option<String>,
    pub attribution_confidence: f32,
}

#[derive(Debug, Clone, Default)]
pub struct ContextProfile {
    pub total_events: usize,
    pub by_category: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct ContextProfiler {
    pub events: Vec<SessionEvent>,
    seen_hashes: std::collections::HashSet<u64>,
}

impl ContextProfiler {
    pub fn new() -> Self {
        Self { events: Vec::new(), seen_hashes: std::collections::HashSet::new() }
    }

    pub fn record(&mut self, mut event: SessionEvent) {
        event.data_hash = hash_str(&event.data_summary);
        self.events.push(event);
    }

    /// Deduplicated record: skips if same hash already seen
    pub fn record_dedup(&mut self, mut event: SessionEvent) -> bool {
        event.data_hash = hash_str(&event.data_summary);
        if self.seen_hashes.contains(&event.data_hash) { return false; }
        self.seen_hashes.insert(event.data_hash);
        self.events.push(event);
        true
    }

    pub fn profile(&self) -> ContextProfile {
        let mut p = ContextProfile::default();
        for e in &self.events {
            *p.by_category.entry(e.category.clone()).or_default() += 1;
        }
        p.total_events = self.events.len();
        p
    }

    pub fn token_distribution(&self) -> String {
        let profile = self.profile();
        let mut parts: Vec<String> = profile.by_category.iter()
            .map(|(cat, count)| format!("{}:{}", cat, count))
            .collect();
        parts.sort();
        parts.join(" ")
    }
}

fn hash_str(s: &str) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

impl Default for SessionEvent {
    fn default() -> Self {
        Self {
            event_type: String::new(), category: String::new(), data_summary: String::new(),
            priority: 5, data_hash: 0, timestamp: 0, project_dir: None, attribution_confidence: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(cat: &str, data: &str) -> SessionEvent {
        SessionEvent { event_type: "test".into(), category: cat.into(), data_summary: data.into(), ..Default::default() }
    }

    #[test]
    fn t01_record_increases_count() {
        let mut p = ContextProfiler::new();
        p.record(make_event("tool", "bash"));
        assert_eq!(p.events.len(), 1);
    }

    #[test]
    fn t01_profile_by_category() {
        let mut p = ContextProfiler::new();
        p.record(make_event("tool", "bash"));
        p.record(make_event("tool", "read"));
        p.record(make_event("message", "hello"));
        let profile = p.profile();
        assert_eq!(profile.by_category.get("tool").unwrap(), &2);
        assert_eq!(profile.by_category.get("message").unwrap(), &1);
    }

    #[test]
    fn t01_empty_profiler() {
        let p = ContextProfiler::new();
        assert_eq!(p.profile().total_events, 0);
    }

    #[test]
    fn t01_hash_consistency() {
        let _p = ContextProfiler::new();
        let h1 = hash_str("same data");
        let h2 = hash_str("same data");
        assert_eq!(h1, h2);
    }

    #[test]
    fn t01_dedup_skips_duplicate() {
        let mut p = ContextProfiler::new();
        assert!(p.record_dedup(make_event("tool", "unique")));
        assert!(!p.record_dedup(make_event("tool", "unique")));
        assert_eq!(p.events.len(), 1);
    }
}