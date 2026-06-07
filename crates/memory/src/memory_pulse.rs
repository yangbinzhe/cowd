//! Memory pulse consumer.
//!
//! The consumer accepts reviewable memory candidates from runtime events
//! without mutating authoritative memory directly. It writes candidates into
//! the maintenance queue so humans or explicit policies can apply/dismiss them.

use serde::{Deserialize, Serialize};

use crate::error::MemoryError;
use crate::maintenance::{MaintenanceCandidate, MaintenanceQueue};
use crate::runtime_event::{RuntimeEvent, RuntimeEventScope};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPulseConfig {
    pub enabled: bool,
    pub max_candidates_per_batch: usize,
}

impl Default for MemoryPulseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_candidates_per_batch: 64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPulseBatch {
    pub source_event_id: String,
    pub source_ref: String,
    pub candidates: Vec<MaintenanceCandidate>,
}

impl MemoryPulseBatch {
    pub fn from_runtime_event(event: &RuntimeEvent) -> Option<Self> {
        if !matches!(
            event.scope,
            RuntimeEventScope::Agent | RuntimeEventScope::Workgraph | RuntimeEventScope::Memory
        ) {
            return None;
        }

        let candidates = runtime_event_candidates(event)?;
        if candidates.is_empty() {
            return None;
        }

        Some(Self {
            source_event_id: event.event_id.clone(),
            source_ref: runtime_event_source_ref(event),
            candidates,
        })
    }
}

fn runtime_event_candidates(event: &RuntimeEvent) -> Option<Vec<MaintenanceCandidate>> {
    let payload = &event.payload;
    if let Some(value) = payload.get("maintenance_candidates") {
        return serde_json::from_value::<Vec<MaintenanceCandidate>>(value.clone()).ok();
    }
    if let Some(value) = payload.get("candidates") {
        return serde_json::from_value::<Vec<MaintenanceCandidate>>(value.clone()).ok();
    }
    if let Some(value) = payload
        .get("review_packet")
        .and_then(|packet| packet.get("maintenance_candidates"))
    {
        return serde_json::from_value::<Vec<MaintenanceCandidate>>(value.clone()).ok();
    }
    None
}

fn runtime_event_source_ref(event: &RuntimeEvent) -> String {
    event
        .refs
        .iter()
        .find(|reference| {
            matches!(
                reference.ref_type.as_str(),
                "workgraph" | "collaboration_board" | "agent_runtime_run"
            )
        })
        .map(|reference| reference.id.clone())
        .or_else(|| {
            event
                .payload
                .get("board_id")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| event.event_id.clone())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPulseTransition {
    pub candidate_id: String,
    pub action: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPulseReport {
    pub source_event_id: String,
    pub accepted: usize,
    pub dropped: usize,
    pub queue_inserted: usize,
    pub degraded_reason: Option<String>,
    pub transitions: Vec<MemoryPulseTransition>,
}

#[derive(Debug, Clone)]
pub struct MemoryPulseConsumer {
    queue: MaintenanceQueue,
    config: MemoryPulseConfig,
}

impl MemoryPulseConsumer {
    #[must_use]
    pub fn new(queue: MaintenanceQueue) -> Self {
        Self {
            queue,
            config: MemoryPulseConfig::default(),
        }
    }

    #[must_use]
    pub fn with_config(mut self, config: MemoryPulseConfig) -> Self {
        self.config = config;
        self
    }

    pub fn process_batch(
        &self,
        mut batch: MemoryPulseBatch,
    ) -> Result<MemoryPulseReport, MemoryError> {
        if !self.config.enabled {
            let dropped = batch.candidates.len();
            return Ok(MemoryPulseReport {
                source_event_id: batch.source_event_id,
                accepted: 0,
                dropped,
                queue_inserted: 0,
                degraded_reason: Some("memory pulse consumer disabled".to_string()),
                transitions: Vec::new(),
            });
        }

        let max = self.config.max_candidates_per_batch.max(1);
        let dropped = batch.candidates.len().saturating_sub(max);
        batch.candidates.truncate(max);
        for candidate in &mut batch.candidates {
            candidate.source = Some("memory_pulse".to_string());
            candidate.source_ref = Some(batch.source_ref.clone());
        }

        let transitions = batch
            .candidates
            .iter()
            .map(|candidate| MemoryPulseTransition {
                candidate_id: candidate.id.clone(),
                action: "queued_for_review".to_string(),
            })
            .collect::<Vec<_>>();
        let accepted = batch.candidates.len();
        let queue_inserted = self.queue.upsert_many(batch.candidates)?;

        Ok(MemoryPulseReport {
            source_event_id: batch.source_event_id,
            accepted,
            dropped,
            queue_inserted,
            degraded_reason: (dropped > 0)
                .then(|| "candidate batch exceeded configured budget".to_string()),
            transitions,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::maintenance::{
        MaintenanceCandidateFilter, MaintenanceCandidateKind, MaintenanceCandidateStatus,
    };
    use crate::runtime_event::{RuntimeEventScope, RuntimeRef};

    fn candidate(id: &str) -> MaintenanceCandidate {
        MaintenanceCandidate {
            id: id.to_string(),
            kind: MaintenanceCandidateKind::RelationshipRefresh,
            status: MaintenanceCandidateStatus::Open,
            entry_ids: Vec::new(),
            summary: format!("candidate {id}"),
            reason: "agent pulse".to_string(),
            confidence: 0.7,
            source: None,
            source_ref: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn memory_pulse_processes_candidates_without_blocking() {
        let queue = MaintenanceQueue::new();
        let consumer = MemoryPulseConsumer::new(queue.clone());
        let report = consumer
            .process_batch(MemoryPulseBatch {
                source_event_id: "event-1".to_string(),
                source_ref: "board-1".to_string(),
                candidates: vec![candidate("c1"), candidate("c2")],
            })
            .unwrap();

        assert_eq!(report.accepted, 2);
        assert_eq!(report.queue_inserted, 2);
        let queued = queue
            .list(MaintenanceCandidateFilter::default())
            .expect("queue list");
        assert_eq!(queued.len(), 2);
        assert!(queued
            .iter()
            .all(|candidate| candidate.source.as_deref() == Some("memory_pulse")));
    }

    #[test]
    fn memory_pulse_records_auditable_transitions() {
        let consumer = MemoryPulseConsumer::new(MaintenanceQueue::new());
        let report = consumer
            .process_batch(MemoryPulseBatch {
                source_event_id: "event-2".to_string(),
                source_ref: "board-2".to_string(),
                candidates: vec![candidate("audit-candidate")],
            })
            .unwrap();

        assert_eq!(report.transitions.len(), 1);
        assert_eq!(report.transitions[0].candidate_id, "audit-candidate");
        assert_eq!(report.transitions[0].action, "queued_for_review");
    }

    #[test]
    fn memory_pulse_degrades_when_batch_exceeds_budget() {
        let consumer =
            MemoryPulseConsumer::new(MaintenanceQueue::new()).with_config(MemoryPulseConfig {
                enabled: true,
                max_candidates_per_batch: 1,
            });
        let report = consumer
            .process_batch(MemoryPulseBatch {
                source_event_id: "event-3".to_string(),
                source_ref: "board-3".to_string(),
                candidates: vec![candidate("c1"), candidate("c2")],
            })
            .unwrap();

        assert_eq!(report.accepted, 1);
        assert_eq!(report.dropped, 1);
        assert_eq!(
            report.degraded_reason.as_deref(),
            Some("candidate batch exceeded configured budget")
        );
    }

    #[test]
    fn memory_pulse_batch_extracts_candidates_from_runtime_event() {
        let event = RuntimeEvent {
            event_id: "event-memory".to_string(),
            session_id: "session-1".to_string(),
            sequence: 4,
            scope: RuntimeEventScope::Workgraph,
            kind: "agent.workgraph.reviewed".to_string(),
            span_id: None,
            parent_span_id: None,
            correlation_id: None,
            status: Some("completed".to_string()),
            refs: vec![RuntimeRef {
                ref_type: "collaboration_board".to_string(),
                id: "board-1".to_string(),
                label: None,
            }],
            payload: serde_json::json!({
                "maintenance_candidates": [candidate("from-event")]
            }),
            created_at_ms: 42,
        };

        let batch = MemoryPulseBatch::from_runtime_event(&event).unwrap();
        assert_eq!(batch.source_event_id, "event-memory");
        assert_eq!(batch.source_ref, "board-1");
        assert_eq!(batch.candidates.len(), 1);
        assert_eq!(batch.candidates[0].id, "from-event");
    }

    #[test]
    fn memory_pulse_batch_ignores_irrelevant_runtime_event() {
        let event = RuntimeEvent::new(
            "session-1",
            7,
            RuntimeEventScope::Tool,
            "tool.completed",
            serde_json::json!({"maintenance_candidates": [candidate("ignored")]}),
            77,
        );

        assert!(MemoryPulseBatch::from_runtime_event(&event).is_none());
    }
}
