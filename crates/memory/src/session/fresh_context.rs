//! Fresh Context Manager - implements GSD-style fresh context priority.
//!
//! This module provides cross-session fresh context management:
//! 1. Session-scoped token windows (each session has its own budget)
//! 2. Freshness-priority loading (newer context has higher priority)
//! 3. Automatic handoff recovery (restore state from previous sessions)
//!
//! ## Design
//!
//! Unlike traditional systems where all context accumulates in one window,
//! this implementation gives each session its own token budget and prioritizes
//! fresh context over older context.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::MemoryError;
use crate::handoff::HandoffManager;
use crate::types::{MemoryEntry, MemoryLayer, Priority};
use crate::MemoryScope;

/// Result type for fresh context operations.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// Token budget for a single session.
#[derive(Debug, Clone)]
pub struct SessionTokenBudget {
    /// Total tokens allocated for this session
    pub total: u64,
    /// Tokens used by memory entries
    pub used: u64,
    /// Freshness threshold: entries older than this are deprioritized
    pub freshness_threshold_hours: u64,
}

impl SessionTokenBudget {
    /// Create a new session budget with default settings.
    pub fn new(total: u64) -> Self {
        Self {
            total,
            used: 0,
            freshness_threshold_hours: 24, // Entries older than 24h are considered stale
        }
    }

    /// Check if budget has remaining capacity.
    pub fn has_capacity(&self, tokens: u64) -> bool {
        self.used + tokens <= self.total
    }

    /// Allocate tokens from the budget.
    pub fn allocate(&mut self, tokens: u64) -> bool {
        if self.has_capacity(tokens) {
            self.used += tokens;
            true
        } else {
            false
        }
    }

    /// Get remaining capacity.
    pub fn remaining(&self) -> u64 {
        self.total.saturating_sub(self.used)
    }

    /// Get usage ratio (0.0 - 1.0).
    pub fn usage_ratio(&self) -> f32 {
        if self.total == 0 {
            1.0
        } else {
            self.used as f32 / self.total as f32
        }
    }
}

/// A memory entry with freshness metadata for prioritization.
#[derive(Debug, Clone)]
pub struct FreshEntry {
    /// The memory entry itself.
    pub entry: MemoryEntry,
    /// Freshness score (higher = more recent/more important).
    /// Based on: creation time, access frequency, priority level.
    pub freshness_score: f32,
    /// Whether this entry came from a handoff.
    pub from_handoff: bool,
}

impl FreshEntry {
    /// Calculate freshness score based on entry metadata.
    pub fn from_entry(entry: MemoryEntry, now_secs: i64) -> Self {
        let age_hours = (now_secs - entry.created_at.timestamp()) as f32 / 3600.0;
        let access_boost = (entry.access_count as f32 * 0.1).min(2.0); // Max 2.0 boost
        let priority_boost = match entry.priority {
            Priority::Critical => 5.0,
            Priority::High => 3.0,
            Priority::Normal => 1.0,
            Priority::Low => 0.5,
        };

        // Exponential decay: fresher entries score higher
        let decay_factor = (-age_hours / 72.0).exp(); // Half-life ~50 hours
        let freshness_score = (1.0 + priority_boost + access_boost) * decay_factor;

        Self {
            entry,
            freshness_score,
            from_handoff: false,
        }
    }

    /// Create from handoff entry with bonus score.
    pub fn from_handoff(entry: MemoryEntry, now_secs: i64) -> Self {
        let mut fresh = Self::from_entry(entry, now_secs);
        fresh.from_handoff = true;
        // Handoff entries get a 50% freshness bonus
        fresh.freshness_score *= 1.5;
        fresh
    }
}

/// Fresh Context Manager - manages session-scoped token windows with freshness priority.
pub struct FreshContextManager {
    /// Session-scoped token budgets.
    budgets: RwLock<HashMap<String, Arc<SessionTokenBudget>>>,
    /// Handoff manager for cross-session recovery.
    handoff_mgr: HandoffManager,
    /// Default budget per session.
    default_budget: u64,
}

impl FreshContextManager {
    /// Create a new fresh context manager.
    pub fn new(default_budget: u64) -> Self {
        Self {
            budgets: RwLock::new(HashMap::new()),
            handoff_mgr: HandoffManager::new(),
            default_budget,
        }
    }

    /// Create a budget for a new session.
    pub async fn create_session_budget(&self, session_id: &str) -> Arc<SessionTokenBudget> {
        let budget = Arc::new(SessionTokenBudget::new(self.default_budget));
        let mut budgets = self.budgets.write().await;
        budgets.insert(session_id.to_string(), Arc::clone(&budget));
        budget
    }

    /// Get or create budget for a session.
    pub async fn get_or_create_budget(&self, session_id: &str) -> Arc<SessionTokenBudget> {
        let budgets = self.budgets.read().await;
        if let Some(budget) = budgets.get(session_id) {
            Arc::clone(budget)
        } else {
            drop(budgets);
            self.create_session_budget(session_id).await
        }
    }

    /// Remove budget for a session (when session ends).
    pub async fn remove_session(&self, session_id: &str) {
        let mut budgets = self.budgets.write().await;
        budgets.remove(session_id);
    }

    /// Load fresh entries for a session, respecting token budget.
    ///
    /// Entries are sorted by freshness score and loaded until budget is exhausted.
    pub async fn load_fresh_entries(
        &self,
        session_id: &str,
        entries: Vec<MemoryEntry>,
        max_entries: usize,
    ) -> Vec<MemoryEntry> {
        let now_secs = chrono::Utc::now().timestamp();

        // Convert to fresh entries with scores
        let mut fresh_entries: Vec<FreshEntry> = entries
            .into_iter()
            .take(max_entries)
            .map(|e| FreshEntry::from_entry(e, now_secs))
            .collect();

        // Sort by freshness score (descending)
        fresh_entries.sort_by(|a, b| {
            b.freshness_score
                .partial_cmp(&a.freshness_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Get budget and allocate entries - need write lock to update
        let mut budgets = self.budgets.write().await;
        let budget = budgets
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(SessionTokenBudget::new(self.default_budget)));

        // Clone the inner budget to work with it mutably
        let mut budget_inner = (**budget).clone();

        let mut result = Vec::new();
        for fresh in fresh_entries {
            let tokens = (fresh.entry.content.len() as u64 / 4).max(1);
            if budget_inner.allocate(tokens) {
                result.push(fresh.entry);
            } else {
                break; // Budget exhausted
            }
        }

        // Update the budget in the map
        budgets.insert(session_id.to_string(), Arc::new(budget_inner));

        result
    }

    /// Recover entries from handoff for a session.
    ///
    /// This implements the GSD-style handoff recovery:
    /// 1. Look for handoff files from previous sessions
    /// 2. Extract high-priority work items and decisions
    /// 3. Convert to fresh memory entries
    pub async fn recover_from_handoff(&self, session_id: &str) -> Result<Vec<MemoryEntry>> {
        // Try to find the latest handoff file
        if let Ok(Some(handoff_data)) = self.handoff_mgr.load_latest() {
            let now_secs = chrono::Utc::now().timestamp();
            let mut entries = Vec::new();

            // Convert work items to entries
            for item in handoff_data.work_items {
                let content = format!(
                    "## Work Item: {}\n{}\n\n**Status:** {:?}\n**Priority:** {:?}",
                    item.title, item.description, item.status, item.priority
                );
                entries.push(MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L1,
                    category: crate::types::MemoryCategory::Reference,
                    priority: item.priority,
                    source: crate::types::MemorySource::Import,
                    title: item.title,
                    content,
                    embedding: None,
                    tags: vec!["handoff".to_string(), "work-item".to_string()],
                    relations: vec![],
                    confidence: 0.9,
                    access_count: 1,
                    staleness: 0.0,
                    created_at: chrono::DateTime::from_timestamp(now_secs, 0).unwrap_or_default(),
                    updated_at: chrono::DateTime::from_timestamp(now_secs, 0).unwrap_or_default(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: Some(session_id.to_string()),
                    source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                });
            }

            // Convert decisions to entries
            for decision in handoff_data.decisions {
                let content = format!(
                    "## Decision: {}\n{}\n\n**Status:** {:?}\n**Made:** {}",
                    decision.summary, decision.rationale, decision.status, decision.made_at
                );
                entries.push(MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L2,
                    category: crate::types::MemoryCategory::Decision,
                    priority: Priority::High,
                    source: crate::types::MemorySource::Import,
                    title: decision.summary,
                    content,
                    embedding: None,
                    tags: vec!["handoff".to_string(), "decision".to_string()],
                    relations: vec![],
                    confidence: 0.95,
                    access_count: 1,
                    staleness: 0.0,
                    created_at: chrono::DateTime::from_timestamp(now_secs, 0).unwrap_or_default(),
                    updated_at: chrono::DateTime::from_timestamp(now_secs, 0).unwrap_or_default(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: Some(session_id.to_string()),
                    source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                });
            }

            // Convert blockers to entries
            for blocker in handoff_data.blockers {
                let content = format!(
                    "## Blocker: {}\n{}",
                    blocker.description,
                    blocker.resolution_hint.unwrap_or_default()
                );
                entries.push(MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L1,
                    category: crate::types::MemoryCategory::Reference,
                    priority: Priority::Critical,
                    source: crate::types::MemorySource::Import,
                    title: format!(
                        "Blocker: {}",
                        blocker.description.chars().take(50).collect::<String>()
                    ),
                    content,
                    embedding: None,
                    tags: vec!["handoff".to_string(), "blocker".to_string()],
                    relations: vec![],
                    confidence: 1.0,
                    access_count: 1,
                    staleness: 0.0,
                    created_at: chrono::DateTime::from_timestamp(now_secs, 0).unwrap_or_default(),
                    updated_at: chrono::DateTime::from_timestamp(now_secs, 0).unwrap_or_default(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: Some(session_id.to_string()),
                    source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                });
            }

            return Ok(entries);
        }

        Ok(Vec::new())
    }

    /// Get current budget status for a session.
    pub async fn get_budget_status(&self, session_id: &str) -> Option<SessionBudgetStatus> {
        let budgets = self.budgets.read().await;
        budgets.get(session_id).map(|budget| SessionBudgetStatus {
            total: budget.total,
            used: budget.used,
            remaining: budget.remaining(),
            usage_ratio: budget.usage_ratio(),
        })
    }
}

/// Budget status for a session.
#[derive(Debug, Clone)]
pub struct SessionBudgetStatus {
    pub total: u64,
    pub used: u64,
    pub remaining: u64,
    pub usage_ratio: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MemoryLayer, MemorySource, Priority};

    #[tokio::test]
    async fn test_session_budget() {
        let mut budget = SessionTokenBudget::new(1000);

        assert!(budget.has_capacity(500));
        assert!(!budget.has_capacity(1500));

        assert!(budget.allocate(500));
        assert_eq!(budget.used, 500);
        assert_eq!(budget.remaining(), 500);

        assert!(!budget.allocate(600)); // Can't allocate, would exceed
        assert_eq!(budget.usage_ratio(), 0.5);
    }

    #[tokio::test]
    async fn test_fresh_entry_scoring() {
        let now_secs = chrono::Utc::now().timestamp();

        let recent_entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L1,
            category: crate::types::MemoryCategory::Reference,
            priority: Priority::High,
            source: MemorySource::UserExplicit,
            title: "Recent".to_string(),
            content: "Test".to_string(),
            embedding: None,
            tags: vec![],
            relations: vec![],
            confidence: 1.0,
            access_count: 5,
            staleness: 0.0,
            created_at: chrono::DateTime::from_timestamp(now_secs - 3600, 0).unwrap_or_default(),
            updated_at: chrono::DateTime::from_timestamp(now_secs - 3600, 0).unwrap_or_default(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        };

        let fresh = FreshEntry::from_entry(recent_entry.clone(), now_secs);
        assert!(fresh.freshness_score > 0.0);

        // Critical priority should score higher
        let critical = MemoryEntry {
            priority: Priority::Critical,
            ..recent_entry
        };
        let critical_fresh = FreshEntry::from_entry(critical, now_secs);
        assert!(critical_fresh.freshness_score > fresh.freshness_score);
    }

    #[tokio::test]
    async fn test_fresh_context_manager() {
        let manager = FreshContextManager::new(500);

        // Create session budget
        let budget1 = manager.create_session_budget("session1").await;
        assert_eq!(budget1.total, 500);

        // Get same budget
        let budget2 = manager.get_or_create_budget("session1").await;
        assert!(Arc::ptr_eq(&budget1, &budget2));

        // Get different session creates new budget
        let budget3 = manager.get_or_create_budget("session2").await;
        assert!(!Arc::ptr_eq(&budget1, &budget3));

        // Remove session
        manager.remove_session("session1").await;
        let status = manager.get_budget_status("session1").await;
        assert!(status.is_none());
    }
}
