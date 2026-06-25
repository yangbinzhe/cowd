use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::MemoryId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryUsageSignal {
    pub memory_id: MemoryId,
    pub session_id: String,
    pub agent_id: String,
    pub selected_count: u64,
    pub last_reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryUsageSummary {
    pub total_selected: u64,
    pub hot_memory_ids: Vec<MemoryId>,
    pub per_memory_selected: HashMap<MemoryId, u64>,
}

#[must_use]
pub fn summarize_usage(signals: &[MemoryUsageSignal], hot_threshold: u64) -> MemoryUsageSummary {
    let mut summary = MemoryUsageSummary::default();
    for signal in signals {
        summary.total_selected = summary.total_selected.saturating_add(signal.selected_count);
        *summary
            .per_memory_selected
            .entry(signal.memory_id)
            .or_insert(0) += signal.selected_count;
    }
    let mut hot = summary
        .per_memory_selected
        .iter()
        .filter_map(|(id, count)| (*count >= hot_threshold).then_some((*id, *count)))
        .collect::<Vec<_>>();
    hot.sort_by(|a, b| b.1.cmp(&a.1));
    summary.hot_memory_ids = hot.into_iter().map(|(id, _)| id).collect();
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn repeated_context_selection_marks_hot_memory() {
        let memory_id = Uuid::new_v4();
        let summary = summarize_usage(
            &[
                MemoryUsageSignal {
                    memory_id,
                    session_id: "s".to_string(),
                    agent_id: "a".to_string(),
                    selected_count: 2,
                    last_reason: "orientation".to_string(),
                },
                MemoryUsageSignal {
                    memory_id,
                    session_id: "s".to_string(),
                    agent_id: "a".to_string(),
                    selected_count: 2,
                    last_reason: "supporting".to_string(),
                },
            ],
            3,
        );

        assert_eq!(summary.total_selected, 4);
        assert_eq!(summary.hot_memory_ids, vec![memory_id]);
    }
}
