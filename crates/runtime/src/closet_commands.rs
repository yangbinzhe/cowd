//! Slash-command handlers for the `/closet` command.
//!
//! Provides formatting and query logic for exploring memory rooms (closets)
//! built from the memory orchestrator's L2/L3 layers.

use memory::closet::{ClosetManager, RANK_BOOSTS};

/// Result of processing a `/closet` command.
pub enum ClosetCommandResult {
    /// List of all topics with their drawer counts.
    TopicList {
        topics: Vec<String>,
        counts: Vec<usize>,
    },
    /// Detail for a specific topic.
    TopicDetail {
        topic: String,
        pointers: Vec<ClosetPointerInfo>,
    },
}

/// Display info for a single closet pointer row.
pub struct ClosetPointerInfo {
    pub entities: Vec<String>,
    pub drawer_count: usize,
    pub relevance_score: f64,
    pub rank_boost: f64,
}

/// Handle `/closet` (no arguments) — list all memory room topics.
#[must_use]
pub fn handle_closet_list(manager: &ClosetManager) -> ClosetCommandResult {
    let pointers = manager.list_topics();
    let topics: Vec<String> = pointers.iter().map(|p| p.topic.clone()).collect();
    let counts: Vec<usize> = pointers.iter().map(|p| p.drawer_ids.len()).collect();
    ClosetCommandResult::TopicList { topics, counts }
}

/// Handle `/closet <topic>` — show detail for a specific topic.
#[must_use]
pub fn handle_closet_topic(manager: &ClosetManager, topic: &str) -> ClosetCommandResult {
    let pointers = manager.get_pointers_for_topic(topic);
    let infos: Vec<ClosetPointerInfo> = pointers
        .iter()
        .enumerate()
        .map(|(i, p)| ClosetPointerInfo {
            entities: p.entities.clone(),
            drawer_count: p.drawer_ids.len(),
            relevance_score: p.relevance_score,
            rank_boost: RANK_BOOSTS.get(i).copied().unwrap_or(0.0),
        })
        .collect();
    ClosetCommandResult::TopicDetail {
        topic: topic.to_string(),
        pointers: infos,
    }
}

/// Format a [`ClosetCommandResult`] as a Markdown string suitable for display.
#[must_use]
pub fn format_closet_result(result: &ClosetCommandResult) -> String {
    match result {
        ClosetCommandResult::TopicList { topics, counts } => {
            if topics.is_empty() {
                return "No memory rooms found. Start building project memories first.".to_string();
            }
            let mut output = String::from("## Memory Rooms (Closets)\n\n");
            output.push_str("| Topic | Drawers |\n");
            output.push_str("|-------|--------|\n");
            for (topic, count) in topics.iter().zip(counts.iter()) {
                output.push_str(&format!("| `{}` | {} |\n", topic, count));
            }
            output.push_str("\nUse `/closet <topic>` to explore a room.\n");
            output
        }
        ClosetCommandResult::TopicDetail { topic, pointers } => {
            let mut output = format!("## Closet: `{}`\n\n", topic);
            if pointers.is_empty() {
                output.push_str("No entries found for this topic.\n");
                return output;
            }
            for (i, info) in pointers.iter().enumerate() {
                output.push_str(&format!(
                    "### #{}. Score: {:.2} (boost: +{:.2})\n",
                    i + 1,
                    info.relevance_score,
                    info.rank_boost,
                ));
                output.push_str(&format!("- Entities: {}\n", info.entities.join(", ")));
                output.push_str(&format!("- References: {} drawer(s)\n", info.drawer_count));
                output.push('\n');
            }
            output
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory::closet::{Closet, ClosetEntry};

    fn make_test_manager() -> ClosetManager {
        let entries = vec![
            ClosetEntry {
                id: "1".into(),
                title: "API Design".into(),
                content: "We chose REST over GraphQL for simplicity".into(),
                entities: vec!["REST".into(), "GraphQL".into()],
            },
            ClosetEntry {
                id: "2".into(),
                title: "Database Schema".into(),
                content: "Postgres with UUID primary keys".into(),
                entities: vec!["Postgres".into(), "UUID".into()],
            },
            ClosetEntry {
                id: "3".into(),
                title: "Error Handling".into(),
                content: "REST style errors with structured responses".into(),
                entities: vec!["REST".into()],
            },
        ];
        let closet = Closet::build(&entries);
        ClosetManager::from_closet(closet)
    }

    #[test]
    fn test_list_topics() {
        let mgr = make_test_manager();
        let result = handle_closet_list(&mgr);
        match result {
            ClosetCommandResult::TopicList { topics, .. } => {
                assert!(!topics.is_empty(), "should have topics");
            }
            _ => panic!("expected TopicList"),
        }
    }

    #[test]
    fn test_search_topic() {
        let mgr = make_test_manager();
        let result = handle_closet_topic(&mgr, "rest");
        match result {
            ClosetCommandResult::TopicDetail { pointers, .. } => {
                assert!(!pointers.is_empty(), "should find REST-related pointers");
            }
            _ => panic!("expected TopicDetail"),
        }
    }

    #[test]
    fn test_format_list_output() {
        let mgr = make_test_manager();
        let result = handle_closet_list(&mgr);
        let formatted = format_closet_result(&result);
        assert!(formatted.contains("Memory Rooms"));
        assert!(formatted.contains("Topic"));
    }

    #[test]
    fn test_format_detail_output() {
        let mgr = make_test_manager();
        let result = handle_closet_topic(&mgr, "rest");
        let formatted = format_closet_result(&result);
        assert!(formatted.contains("Closet:"));
    }

    #[test]
    fn test_empty_closet() {
        let closet = Closet::build(&[]);
        let mgr = ClosetManager::from_closet(closet);
        let result = handle_closet_list(&mgr);
        let formatted = format_closet_result(&result);
        assert!(formatted.contains("No memory rooms found"));
    }
}
