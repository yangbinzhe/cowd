//! Per-turn ledger primitives for tool runtime events.
//!
//! The ledger is intentionally independent from storage. It deduplicates and
//! orders runtime events before callers flush them to the session event log.

use std::collections::{BTreeMap, HashSet};

use memory::RuntimeEvent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolLedgerEventKind {
    Plan,
    Schedule,
    InvocationStarted,
    InvocationCompleted,
    InvocationFailed,
    InvocationDenied,
    Message,
    Cache,
    Mutation,
    Checkpoint,
    Warning,
    Other,
}

impl ToolLedgerEventKind {
    #[must_use]
    pub fn from_runtime_kind(kind: &str) -> Self {
        match kind {
            "tool.execution.plan.created" | "tool.execution_plan.created" => Self::Plan,
            "tool.schedule.created" => Self::Schedule,
            "tool.invocation.started" => Self::InvocationStarted,
            "tool.invocation.completed" => Self::InvocationCompleted,
            "tool.invocation.failed" => Self::InvocationFailed,
            "tool.invocation.denied" => Self::InvocationDenied,
            kind if kind.contains("cache") => Self::Cache,
            kind if kind.contains("mutation") => Self::Mutation,
            kind if kind.contains("checkpoint") => Self::Checkpoint,
            kind if kind.contains("warning") => Self::Warning,
            _ => Self::Other,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Schedule => "schedule",
            Self::InvocationStarted => "invocation_started",
            Self::InvocationCompleted => "invocation_completed",
            Self::InvocationFailed => "invocation_failed",
            Self::InvocationDenied => "invocation_denied",
            Self::Message => "message",
            Self::Cache => "cache",
            Self::Mutation => "mutation",
            Self::Checkpoint => "checkpoint",
            Self::Warning => "warning",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolLedgerEvent {
    pub idempotency_key: String,
    pub session_id: String,
    pub sequence: usize,
    pub turn_index: usize,
    pub kind: ToolLedgerEventKind,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
    pub created_at_ms: u64,
    pub runtime_event: RuntimeEvent,
}

impl ToolLedgerEvent {
    #[must_use]
    pub fn from_runtime_event(
        turn_index: usize,
        idempotency_key: impl Into<String>,
        runtime_event: RuntimeEvent,
    ) -> Self {
        let tool_call_id = runtime_event.correlation_id.clone().or_else(|| {
            runtime_event
                .refs
                .iter()
                .find(|reference| reference.ref_type == "tool_call")
                .map(|reference| reference.id.clone())
        });
        let tool_name = runtime_event
            .refs
            .iter()
            .find(|reference| reference.ref_type == "tool")
            .map(|reference| reference.id.clone())
            .or_else(|| {
                runtime_event
                    .refs
                    .iter()
                    .find(|reference| reference.ref_type == "tool_call")
                    .and_then(|reference| reference.label.clone())
            });

        Self {
            idempotency_key: idempotency_key.into(),
            session_id: runtime_event.session_id.clone(),
            sequence: runtime_event.sequence,
            turn_index,
            kind: ToolLedgerEventKind::from_runtime_kind(&runtime_event.kind),
            tool_call_id,
            tool_name,
            created_at_ms: runtime_event.created_at_ms,
            runtime_event,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolLedgerStats {
    pub event_count: usize,
    pub duplicate_count: usize,
    pub kind_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolLedgerFlush {
    pub events: Vec<RuntimeEvent>,
    pub stats: ToolLedgerStats,
}

#[derive(Debug, Clone)]
pub struct TurnToolLedger {
    session_id: String,
    turn_index: usize,
    events: Vec<(usize, ToolLedgerEvent)>,
    seen_keys: HashSet<String>,
    duplicate_count: usize,
    next_insertion_order: usize,
}

impl TurnToolLedger {
    #[must_use]
    pub fn new(session_id: impl Into<String>, turn_index: usize) -> Self {
        Self {
            session_id: session_id.into(),
            turn_index,
            events: Vec::new(),
            seen_keys: HashSet::new(),
            duplicate_count: 0,
            next_insertion_order: 0,
        }
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub const fn turn_index(&self) -> usize {
        self.turn_index
    }

    pub fn append(&mut self, event: ToolLedgerEvent) -> bool {
        if !self.seen_keys.insert(event.idempotency_key.clone()) {
            self.duplicate_count = self.duplicate_count.saturating_add(1);
            return false;
        }
        let insertion_order = self.next_insertion_order;
        self.next_insertion_order = self.next_insertion_order.saturating_add(1);
        self.events.push((insertion_order, event));
        true
    }

    pub fn append_runtime_event(
        &mut self,
        idempotency_key: impl Into<String>,
        runtime_event: RuntimeEvent,
    ) -> bool {
        self.append(ToolLedgerEvent::from_runtime_event(
            self.turn_index,
            idempotency_key,
            runtime_event,
        ))
    }

    #[must_use]
    pub fn stats(&self) -> ToolLedgerStats {
        let mut kind_counts = BTreeMap::new();
        for (_, event) in &self.events {
            *kind_counts
                .entry(event.kind.as_str().to_string())
                .or_insert(0) += 1;
        }
        ToolLedgerStats {
            event_count: self.events.len(),
            duplicate_count: self.duplicate_count,
            kind_counts,
        }
    }

    #[must_use]
    pub fn flush(mut self) -> ToolLedgerFlush {
        self.events.sort_by(|left, right| {
            let left_event = &left.1;
            let right_event = &right.1;
            left_event
                .sequence
                .cmp(&right_event.sequence)
                .then(left_event.created_at_ms.cmp(&right_event.created_at_ms))
                .then(left.0.cmp(&right.0))
        });
        let stats = self.stats();
        let events = self
            .events
            .into_iter()
            .map(|(_, event)| event.runtime_event)
            .collect();
        ToolLedgerFlush { events, stats }
    }
}

#[must_use]
pub fn tool_event_idempotency_key(event: &RuntimeEvent) -> String {
    let correlation = event.correlation_id.as_deref().unwrap_or("-");
    let span = event.span_id.as_deref().unwrap_or("-");
    format!(
        "{}::{}::{}::{}::{}",
        event.session_id, event.sequence, event.kind, correlation, span
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
    use serde_json::json;

    fn event(sequence: usize, kind: &str, created_at_ms: u64) -> RuntimeEvent {
        let mut event = RuntimeEvent::new(
            "session-1",
            sequence,
            RuntimeEventScope::Tool,
            kind,
            json!({"ok": true}),
            created_at_ms,
        );
        event.correlation_id = Some("tool-call-1".to_string());
        event.refs = vec![RuntimeRef {
            ref_type: "tool".to_string(),
            id: "read_file".to_string(),
            label: None,
        }];
        event
    }

    #[test]
    fn tool_ledger_deduplicates_by_idempotency_key() {
        let mut ledger = TurnToolLedger::new("session-1", 3);
        let event = event(7, "tool.invocation.started", 100);

        assert!(ledger.append_runtime_event("same-key", event.clone()));
        assert!(!ledger.append_runtime_event("same-key", event));

        let stats = ledger.stats();
        assert_eq!(stats.event_count, 1);
        assert_eq!(stats.duplicate_count, 1);
    }

    #[test]
    fn tool_ledger_orders_events_stably() {
        let mut ledger = TurnToolLedger::new("session-1", 3);
        ledger.append_runtime_event("b", event(2, "tool.invocation.completed", 200));
        ledger.append_runtime_event("a", event(1, "tool.invocation.started", 300));
        ledger.append_runtime_event("c", event(2, "tool.schedule.created", 100));

        let flushed = ledger.flush();
        let kinds = flushed
            .events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "tool.invocation.started",
                "tool.schedule.created",
                "tool.invocation.completed"
            ]
        );
    }

    #[test]
    fn tool_ledger_keeps_runtime_event_payload_unchanged() {
        let mut ledger = TurnToolLedger::new("session-1", 3);
        let event = event(7, "tool.execution.plan.created", 100);
        let expected_payload = event.payload.clone();
        ledger.append_runtime_event(tool_event_idempotency_key(&event), event);

        let flushed = ledger.flush();
        assert_eq!(flushed.events[0].payload, expected_payload);
        assert_eq!(flushed.events[0].kind, "tool.execution.plan.created");
    }

    #[test]
    fn tool_ledger_reports_stats() {
        let mut ledger = TurnToolLedger::new("session-1", 3);
        ledger.append_runtime_event("plan", event(1, "tool.execution_plan.created", 100));
        ledger.append_runtime_event("schedule", event(2, "tool.schedule.created", 110));
        ledger.append_runtime_event("schedule", event(2, "tool.schedule.created", 110));

        let stats = ledger.stats();
        assert_eq!(stats.event_count, 2);
        assert_eq!(stats.duplicate_count, 1);
        assert_eq!(stats.kind_counts.get("plan"), Some(&1));
        assert_eq!(stats.kind_counts.get("schedule"), Some(&1));
    }
}
