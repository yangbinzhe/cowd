use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::types::{MemoryEntry, MemoryId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryCluster {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub entry_ids: Vec<MemoryId>,
    pub tags: Vec<String>,
    pub token_estimate: u64,
    pub hotness: f32,
    pub truncated: bool,
}

#[must_use]
pub fn cluster_entries(entries: &[MemoryEntry], max_summary_chars: usize) -> Vec<MemoryCluster> {
    let mut groups: HashMap<String, Vec<&MemoryEntry>> = HashMap::new();
    for entry in entries {
        groups.entry(cluster_key(entry)).or_default().push(entry);
    }

    let mut clusters = groups
        .into_iter()
        .map(|(key, mut group)| {
            group.sort_by(|a, b| {
                b.priority
                    .cmp(&a.priority)
                    .then(b.updated_at.cmp(&a.updated_at))
            });
            let title = group
                .first()
                .map(|entry| entry.title.clone())
                .unwrap_or_else(|| key.clone());
            let joined = group
                .iter()
                .take(8)
                .map(|entry| entry.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            let truncated = joined.chars().count() > max_summary_chars;
            let summary = if truncated {
                joined.chars().take(max_summary_chars).collect::<String>()
            } else {
                joined
            };
            let mut tags = BTreeSet::new();
            for entry in &group {
                for tag in &entry.tags {
                    tags.insert(tag.clone());
                }
            }
            let token_estimate = group
                .iter()
                .map(|entry| (entry.content.len() as u64 / 4).max(1))
                .sum();
            let hotness = group
                .iter()
                .map(|entry| entry.access_count as f32 + priority_weight(entry))
                .sum::<f32>();
            MemoryCluster {
                id: format!("cluster:{key}"),
                title,
                summary,
                entry_ids: group.iter().map(|entry| entry.id).collect(),
                tags: tags.into_iter().collect(),
                token_estimate,
                hotness,
                truncated,
            }
        })
        .collect::<Vec<_>>();
    clusters.sort_by(|a, b| {
        b.hotness
            .partial_cmp(&a.hotness)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.title.cmp(&b.title))
    });
    clusters
}

fn cluster_key(entry: &MemoryEntry) -> String {
    if let Some(tag) = entry.tags.first() {
        normalize(tag)
    } else {
        format!("{:?}:{:?}", entry.layer, entry.category).to_ascii_lowercase()
    }
}

fn priority_weight(entry: &MemoryEntry) -> f32 {
    match entry.priority {
        crate::types::Priority::Critical => 4.0,
        crate::types::Priority::High => 3.0,
        crate::types::Priority::Normal => 2.0,
        crate::types::Priority::Low => 1.0,
    }
}

fn normalize(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("-")
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority,
    };
    use chrono::Utc;
    use uuid::Uuid;

    fn entry(title: &str, tag: &str, content: &str) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::Import,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: vec![tag.to_string()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: crate::project_scope::MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::Shared,
        }
    }

    #[test]
    fn large_cluster_keeps_summary_bounded() {
        let entries = vec![
            entry("A", "docs", &"alpha ".repeat(100)),
            entry("B", "docs", &"beta ".repeat(100)),
        ];

        let clusters = cluster_entries(&entries, 80);

        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].entry_ids.len(), 2);
        assert!(clusters[0].summary.len() <= 80);
        assert!(clusters[0].truncated);
    }
}
