// 0511-P0: AAAK Index Layer — symbolic pointers for memory entries.
// Derived from mempalace's AAAK compression dialect.
// Injects compact symbolic slots instead of full entry contents.

use crate::types::{MemoryEntry, MemoryId};
#[cfg(test)]
use crate::types::MemoryLayer;
use std::collections::HashMap;

/// A compact symbolic slot pointing to a full memory entry.
#[derive(Debug, Clone)]
pub struct AaakSlot {
    pub id: String,
    pub layer: u8,
    pub confidence: f32,
    pub summary: String,
    pub entry_id: MemoryId,
}

/// Index of symbolic slots — LLM scans this to find relevant entries,
/// then opens specific drawers (by slot.id) to read full content.
#[derive(Debug, Clone)]
pub struct AaakIndex {
    pub slots: Vec<AaakSlot>,
    pub budget_used: u64,
    pub total_budget: u64,
}

impl AaakIndex {
    /// Build index from memory entries within budget.
    /// Each slot ≈15 tokens. Returns at most (budget/15) slots.
    pub fn from_entries(entries: &[MemoryEntry], budget: u64) -> Self {
        let max_slots = (budget / 15).max(1).min(entries.len() as u64) as usize;
        let mut slots = Vec::with_capacity(max_slots);
        let mut prefix = b'a';
        let mut counter: u32 = 1;

        for entry in entries.iter().take(max_slots) {
            let summary: String = entry.title.chars().take(40).collect();
            slots.push(AaakSlot {
                id: format!("{}{}", prefix as char, counter),
                layer: entry.layer as u8,
                confidence: entry.confidence,
                summary,
                entry_id: entry.id,
            });
            counter += 1;
            if counter > 99 { prefix += 1; counter = 1; }
        }

        AaakIndex {
            budget_used: slots.len() as u64 * 15,
            total_budget: budget,
            slots,
        }
    }

    /// Serialize to XML for LLM consumption.
    pub fn to_xml(&self) -> String {
        let mut xml = format!(
            "<memory_index slots=\"{}\" budget=\"{}/{}\">\n",
            self.slots.len(), self.budget_used, self.total_budget
        );
        for slot in &self.slots {
            xml.push_str(&format!(
                "  <s id=\"{}\" l=\"{}\" c=\"{:.2}\">{}</s>\n",
                slot.id, slot.layer, slot.confidence, slot.summary
            ));
        }
        xml.push_str("</memory_index>");
        xml
    }

    /// Find full entry by slot id for "drawer opening".
    pub fn resolve<'a>(&self, slot_id: &str, entries: &'a [MemoryEntry]) -> Option<&'a MemoryEntry> {
        self.slots.iter().find(|s| s.id == slot_id)
            .and_then(|s| entries.iter().find(|e| e.id == s.entry_id))
    }

    /// Build a simple lookup map: slot_id → entry index.
    pub fn lookup_map(&self) -> HashMap<String, usize> {
        self.slots.iter().enumerate()
            .map(|(i, s)| (s.id.clone(), i))
            .collect()
    }
}

/// Token estimation for comparison.
pub fn estimate_full_injection_tokens(entries: &[MemoryEntry]) -> u64 {
    entries.iter().map(|e| {
        (e.title.len() + e.content.len()) as u64 / 4
    }).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MemoryCategory, MemorySource, Priority};
    use chrono::Utc;
    use uuid::Uuid;

    fn make_entry(title: &str, content: &str, layer: MemoryLayer, confidence: f32) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::new_v4(), layer, category: MemoryCategory::Reference,
            priority: Priority::Normal, source: MemorySource::Import,
            title: title.into(), content: content.into(),
            embedding: None, tags: vec![], relations: vec![],
            confidence, access_count: 0, staleness: 0.0,
            created_at: Utc::now(), updated_at: Utc::now(),
            last_accessed_at: None, scope: None, session_id: None,
        }
    }

    #[test]
    fn a11_index_builds_slots_within_budget() {
        let entries: Vec<_> = (0..50).map(|i| {
            make_entry(&format!("Entry {}", i), "content", MemoryLayer::L3, 0.5)
        }).collect();
        let index = AaakIndex::from_entries(&entries, 300); // 300/15 = 20 slots
        assert_eq!(index.slots.len(), 20);
        assert!(index.budget_used <= 300);
    }

    #[test]
    fn a11_index_generates_unique_slot_ids() {
        let entries: Vec<_> = (0..30).map(|i| {
            make_entry(&format!("E{}", i), "c", MemoryLayer::L3, 0.5)
        }).collect();
        let index = AaakIndex::from_entries(&entries, 1000);
        let ids: Vec<_> = index.slots.iter().map(|s| s.id.clone()).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(ids.len(), unique.len(), "all slot ids must be unique");
    }

    #[test]
    fn a11_xml_output_contains_slot_tags() {
        let entries = vec![
            make_entry("Memory fix", "fixed bug", MemoryLayer::L0, 0.99),
        ];
        let index = AaakIndex::from_entries(&entries, 100);
        let xml = index.to_xml();
        assert!(xml.contains("<memory_index"));
        assert!(xml.contains("<s id=\"a1\""));
        assert!(xml.contains("</memory_index>"));
    }

    #[test]
    fn a11_resolve_finds_entry_by_slot_id() {
        let entries = vec![
            make_entry("Target", "important data", MemoryLayer::L0, 0.99),
        ];
        let index = AaakIndex::from_entries(&entries, 100);
        let resolved = index.resolve("a1", &entries);
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().title, "Target");
    }

    #[test]
    fn a11_token_savings_ratio() {
        let entries: Vec<_> = (0..20).map(|i| {
            make_entry(&format!("Title {}", i), &"x".repeat(80), MemoryLayer::L3, 0.5)
        }).collect();
        let full_tokens = estimate_full_injection_tokens(&entries);
        let index = AaakIndex::from_entries(&entries, 300);
        assert!(index.budget_used < full_tokens,
            "index tokens ({}) should be less than full ({})",
            index.budget_used, full_tokens);
    }

    #[test]
    fn a11_layer_info_preserved_in_slots() {
        let entries = vec![
            make_entry("L0 entry", "c", MemoryLayer::L0, 0.99),
            make_entry("L3 entry", "c", MemoryLayer::L3, 0.5),
        ];
        let index = AaakIndex::from_entries(&entries, 100);
        assert_eq!(index.slots[0].layer, 0);
        assert_eq!(index.slots[1].layer, 3);
    }
}