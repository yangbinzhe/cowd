use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

pub(crate) const MAX_DRAIN_PAGES_PER_PROBE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObservationPhase {
    Bootstrapping,
    Preparing,
    CallingModel,
    CallingTool,
    WaitingHandoff,
    Finalizing,
    TerminalPending,
    Terminal,
    Quiet,
    Stalled,
}

impl ObservationPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrapping => "bootstrapping",
            Self::Preparing => "preparing",
            Self::CallingModel => "calling_model",
            Self::CallingTool => "calling_tool",
            Self::WaitingHandoff => "waiting_handoff",
            Self::Finalizing => "finalizing",
            Self::TerminalPending => "terminal_pending",
            Self::Terminal => "terminal",
            Self::Quiet => "quiet",
            Self::Stalled => "stalled",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PageObservation {
    pub has_more: bool,
}

#[derive(Debug, Clone, Default)]
struct ChannelStats {
    polls: u64,
    changed: u64,
    unchanged: u64,
    errors: u64,
    bytes_received: u64,
}

impl ChannelStats {
    fn to_value(&self) -> Value {
        json!({
            "polls": self.polls,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "errors": self.errors,
            "bytes_received": self.bytes_received,
        })
    }
}

#[derive(Debug, Clone)]
struct PendingSpan {
    channel: String,
    path: String,
    outcome: String,
    fingerprint: String,
    first_elapsed_ms: u64,
    last_elapsed_ms: u64,
    polls: u64,
    bytes_received: u64,
    detail: Value,
}

impl PendingSpan {
    fn to_value(&self) -> Value {
        json!({
            "kind": "observation_span",
            "channel": self.channel,
            "path": self.path,
            "outcome": self.outcome,
            "fingerprint": self.fingerprint,
            "first_elapsed_ms": self.first_elapsed_ms,
            "last_elapsed_ms": self.last_elapsed_ms,
            "polls": self.polls,
            "bytes_received": self.bytes_received,
            "detail": self.detail,
        })
    }
}

/// Scenario-local reducer for public Gateway observations.
///
/// Runtime and Session remain the canonical state owners. This ledger owns only
/// evaluation cursors, a deduplicated evidence view, progress classification,
/// and a compact trace of changed states plus unchanged/error spans.
pub(crate) struct LiveScenarioObserver {
    next_message_sequence: usize,
    message_payloads: BTreeMap<String, Value>,
    messages: Vec<Value>,
    message_pages_drained: bool,
    timeline_cursor: Option<String>,
    seen_timeline_cursors: BTreeSet<String>,
    timeline_event_payloads: BTreeMap<String, Value>,
    timeline_events: Vec<Value>,
    timeline_template: Map<String, Value>,
    timeline_pages_drained: bool,
    timeline_pages: u64,
    timeline_cursor_advances: u64,
    last_root_fingerprint: Option<String>,
    last_live_fingerprint: Option<String>,
    root_baseline_seen: bool,
    live_baseline_seen: bool,
    message_baseline_seen: bool,
    timeline_baseline_seen: bool,
    phase: ObservationPhase,
    last_active_phase: ObservationPhase,
    last_progress_elapsed_ms: u64,
    progress_observations: u64,
    stall_detected: bool,
    violations: Vec<String>,
    channel_stats: BTreeMap<String, ChannelStats>,
    records: Vec<Value>,
    pending_spans: BTreeMap<String, PendingSpan>,
}

impl Default for LiveScenarioObserver {
    fn default() -> Self {
        Self {
            next_message_sequence: 0,
            message_payloads: BTreeMap::new(),
            messages: Vec::new(),
            message_pages_drained: false,
            timeline_cursor: None,
            seen_timeline_cursors: BTreeSet::new(),
            timeline_event_payloads: BTreeMap::new(),
            timeline_events: Vec::new(),
            timeline_template: Map::new(),
            timeline_pages_drained: false,
            timeline_pages: 0,
            timeline_cursor_advances: 0,
            last_root_fingerprint: None,
            last_live_fingerprint: None,
            root_baseline_seen: false,
            live_baseline_seen: false,
            message_baseline_seen: false,
            timeline_baseline_seen: false,
            phase: ObservationPhase::Bootstrapping,
            last_active_phase: ObservationPhase::Bootstrapping,
            last_progress_elapsed_ms: 0,
            progress_observations: 0,
            stall_detected: false,
            violations: Vec::new(),
            channel_stats: BTreeMap::new(),
            records: Vec::new(),
            pending_spans: BTreeMap::new(),
        }
    }
}

impl LiveScenarioObserver {
    pub(crate) fn message_path(&self, session_id: &str) -> String {
        format!(
            "/api/sessions/{session_id}/messages?from_seq={}&limit=200",
            self.next_message_sequence
        )
    }

    pub(crate) fn timeline_path(&self, session_id: &str) -> String {
        let mut path = format!("/api/runtime/timeline?session_id={session_id}&limit=500");
        if let Some(cursor) = self.timeline_cursor.as_deref() {
            path.push_str("&cursor=");
            path.push_str(cursor);
        }
        path
    }

    pub(crate) const fn next_message_sequence(&self) -> usize {
        self.next_message_sequence
    }

    pub(crate) fn timeline_cursor(&self) -> Option<&str> {
        self.timeline_cursor.as_deref()
    }

    pub(crate) fn observe_message_page(
        &mut self,
        path: &str,
        response: &Result<Value, String>,
        elapsed_ms: u64,
    ) -> Result<PageObservation, String> {
        let bytes = response_bytes(response);
        self.note_poll("messages", bytes, response.is_err());
        let value = match response {
            Ok(value) => value,
            Err(error) => {
                self.message_pages_drained = false;
                self.record_span(
                    "messages",
                    path,
                    "error",
                    error,
                    elapsed_ms,
                    bytes,
                    json!({"error": error}),
                );
                return Ok(PageObservation { has_more: false });
            }
        };
        let Some(object) = value.as_object() else {
            return self.integrity_error("message page is not an object");
        };
        let Some(items) = object.get("messages").and_then(Value::as_array).cloned() else {
            return self.integrity_error("message page lacks messages array");
        };
        let has_more = object
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let current = self.next_message_sequence;
        let next = object
            .get("next_seq")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok());
        if has_more && items.is_empty() {
            return self.integrity_error(
                "message page declares has_more with no items and cannot advance",
            );
        }
        if !items.is_empty() && next.is_none_or(|next| next <= current) {
            return self.integrity_error("nonempty message page did not advance next_seq");
        }
        let mut new_items = Vec::new();
        let mut expected_sequence = current;
        for item in items {
            let Some(sequence) = item
                .get("sequence")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
            else {
                return self.integrity_error("message lacks sequence");
            };
            if sequence != expected_sequence {
                return self.integrity_error(
                    "message sequence was not contiguous from requested from_seq",
                );
            }
            expected_sequence = expected_sequence.saturating_add(1);
            let Some(id) = item
                .get("id")
                .or_else(|| item.get("message_id"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToString::to_string)
            else {
                return self.integrity_error("message lacks stable id");
            };
            if let Some(existing) = self.message_payloads.get(&id) {
                if existing != &item {
                    return self
                        .integrity_error("stable message id was reused with a different payload");
                }
            } else {
                self.message_payloads.insert(id, item.clone());
                self.messages.push(item.clone());
                new_items.push(item);
            }
        }
        if next.is_some_and(|next| next != expected_sequence) {
            return self.integrity_error("message next_seq skipped or replayed a sequence");
        }
        if let Some(next) = next {
            self.next_message_sequence = next;
        }
        self.message_pages_drained = !has_more;
        let changed = !new_items.is_empty();
        let fingerprint = format!(
            "next_seq={}:new={}:has_more={has_more}",
            self.next_message_sequence,
            new_items.len()
        );
        if changed {
            self.record_changed(
                "messages",
                path,
                &fingerprint,
                elapsed_ms,
                bytes,
                json!({
                    "from_seq": current,
                    "next_seq": self.next_message_sequence,
                    "has_more": has_more,
                    "messages": new_items,
                }),
            );
            if self.message_baseline_seen {
                self.mark_progress(elapsed_ms);
            } else {
                self.message_baseline_seen = true;
                self.last_progress_elapsed_ms = elapsed_ms;
            }
        } else {
            self.record_span(
                "messages",
                path,
                "unchanged",
                &fingerprint,
                elapsed_ms,
                bytes,
                json!({"next_seq": self.next_message_sequence}),
            );
        }
        Ok(PageObservation { has_more })
    }

    pub(crate) fn observe_timeline_page(
        &mut self,
        path: &str,
        response: &Result<Value, String>,
        elapsed_ms: u64,
    ) -> Result<PageObservation, String> {
        let bytes = response_bytes(response);
        self.note_poll("timeline", bytes, response.is_err());
        let value = match response {
            Ok(value) => value,
            Err(error) => {
                self.timeline_pages_drained = false;
                self.record_span(
                    "timeline",
                    path,
                    "error",
                    error,
                    elapsed_ms,
                    bytes,
                    json!({"error": error}),
                );
                return Ok(PageObservation { has_more: false });
            }
        };
        let Some(object) = value.as_object() else {
            return self.integrity_error("timeline page is not an object");
        };
        let Some(events) = object.get("events").and_then(Value::as_array).cloned() else {
            return self.integrity_error("timeline page lacks events array");
        };
        let has_more = object
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next_cursor = object
            .get("next_cursor")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string);
        if has_more && events.is_empty() {
            return self.integrity_error(
                "timeline page declares has_more with no events and cannot advance",
            );
        }
        if !events.is_empty() && next_cursor.is_none() {
            return self.integrity_error("nonempty timeline page lacks next_cursor");
        }
        if let Some(next) = next_cursor.as_deref() {
            let next_position =
                parse_timeline_cursor(next).map_err(|error| self.record_integrity_error(error))?;
            if let Some(current) = self.timeline_cursor.as_deref() {
                let current_position = parse_timeline_cursor(current)
                    .map_err(|error| self.record_integrity_error(error))?;
                if !next_position.strictly_advances_from(current_position) {
                    return self.integrity_error("timeline cursor did not advance monotonically");
                }
            }
            if self.timeline_cursor.as_deref() == Some(next)
                || !self.seen_timeline_cursors.insert(next.to_string())
            {
                return self.integrity_error("timeline cursor repeated instead of advancing");
            }
        }
        let mut new_items = Vec::new();
        for event in events {
            let identity = match timeline_event_identity(&event) {
                Ok(identity) => identity,
                Err(error) => return self.integrity_error(&error),
            };
            if let Some(existing) = self.timeline_event_payloads.get(&identity) {
                if existing != &event {
                    return self.integrity_error(
                        "stable timeline event identity carried a different payload",
                    );
                }
            } else {
                self.timeline_event_payloads.insert(identity, event.clone());
                self.timeline_events.push(event.clone());
                new_items.push(event);
            }
        }
        self.timeline_pages = self.timeline_pages.saturating_add(1);
        if let Some(next) = next_cursor {
            self.timeline_cursor = Some(next);
            self.timeline_cursor_advances = self.timeline_cursor_advances.saturating_add(1);
        }
        self.timeline_pages_drained = !has_more;
        self.timeline_template = object.clone();
        self.timeline_template.remove("events");
        let changed = !new_items.is_empty();
        let fingerprint = format!(
            "cursor={}:new={}:total_observed={}:has_more={has_more}",
            self.timeline_cursor.as_deref().unwrap_or("-"),
            new_items.len(),
            self.timeline_events.len()
        );
        if changed {
            self.record_changed(
                "timeline",
                path,
                &fingerprint,
                elapsed_ms,
                bytes,
                json!({
                    "next_cursor": self.timeline_cursor,
                    "has_more": has_more,
                    "events": new_items,
                }),
            );
            if self.timeline_baseline_seen {
                self.mark_progress(elapsed_ms);
            } else {
                self.timeline_baseline_seen = true;
                self.last_progress_elapsed_ms = elapsed_ms;
            }
        } else {
            self.record_span(
                "timeline",
                path,
                "unchanged",
                &fingerprint,
                elapsed_ms,
                bytes,
                json!({"next_cursor": self.timeline_cursor}),
            );
        }
        Ok(PageObservation { has_more })
    }

    pub(crate) fn observe_root(
        &mut self,
        path: &str,
        response: &Result<Value, String>,
        fingerprint: Option<&str>,
        response_body_bytes: Option<u64>,
        elapsed_ms: u64,
    ) -> bool {
        let bytes = response_body_bytes.unwrap_or_else(|| response_bytes(response));
        self.note_poll("root", bytes, response.is_err());
        let Ok(summary) = response else {
            let error = response.as_ref().err().cloned().unwrap_or_default();
            self.phase = ObservationPhase::Bootstrapping;
            self.record_span(
                "root",
                path,
                "error",
                &error,
                elapsed_ms,
                bytes,
                json!({"error": error}),
            );
            return false;
        };
        self.update_phase_from_root(summary);
        let fingerprint = fingerprint.unwrap_or_default();
        let changed = self.last_root_fingerprint.as_deref() != Some(fingerprint);
        if changed {
            self.last_root_fingerprint = Some(fingerprint.to_string());
            self.record_changed(
                "root",
                path,
                fingerprint,
                elapsed_ms,
                bytes,
                summary.clone(),
            );
            if self.root_baseline_seen {
                self.mark_progress(elapsed_ms);
                true
            } else {
                self.root_baseline_seen = true;
                self.last_progress_elapsed_ms = elapsed_ms;
                false
            }
        } else {
            self.record_span(
                "root",
                path,
                "unchanged",
                fingerprint,
                elapsed_ms,
                bytes,
                json!({"phase": self.phase.as_str()}),
            );
            false
        }
    }

    pub(crate) fn observe_live(
        &mut self,
        path: &str,
        response: &Result<Value, String>,
        fingerprint: Option<&str>,
        response_body_bytes: Option<u64>,
        elapsed_ms: u64,
    ) -> bool {
        let bytes = response_body_bytes.unwrap_or_else(|| response_bytes(response));
        self.note_poll("live", bytes, response.is_err());
        let Ok(summary) = response else {
            let error = response.as_ref().err().cloned().unwrap_or_default();
            self.phase = ObservationPhase::Bootstrapping;
            self.record_span(
                "live",
                path,
                "error",
                &error,
                elapsed_ms,
                bytes,
                json!({"error": error}),
            );
            return false;
        };
        self.update_phase_from_root(summary);
        let fingerprint = fingerprint.unwrap_or_default();
        let changed = self.last_live_fingerprint.as_deref() != Some(fingerprint);
        if changed {
            self.last_live_fingerprint = Some(fingerprint.to_string());
            self.record_changed(
                "live",
                path,
                fingerprint,
                elapsed_ms,
                bytes,
                summary.clone(),
            );
            if self.live_baseline_seen {
                self.mark_progress(elapsed_ms);
                true
            } else {
                self.live_baseline_seen = true;
                self.last_progress_elapsed_ms = elapsed_ms;
                false
            }
        } else {
            self.record_span(
                "live",
                path,
                "unchanged",
                fingerprint,
                elapsed_ms,
                bytes,
                json!({"phase": self.phase.as_str()}),
            );
            false
        }
    }

    pub(crate) fn assistant_message(&self) -> Option<Value> {
        self.messages.iter().rev().find_map(|message| {
            (message.get("role").and_then(Value::as_str) == Some("assistant"))
                .then(|| message.clone())
        })
    }

    pub(crate) fn timeline(&self) -> Value {
        let mut value = self.timeline_template.clone();
        let mut events = self.timeline_events.clone();
        events.sort_by_key(timeline_event_order);
        value.insert("events".to_string(), Value::Array(events));
        value.insert("total".to_string(), json!(self.timeline_events.len()));
        value.insert("has_more".to_string(), Value::Bool(false));
        value.insert(
            "next_cursor".to_string(),
            self.timeline_cursor
                .clone()
                .map_or(Value::Null, Value::String),
        );
        value.insert("observation_integrity".to_string(), self.integrity_report());
        Value::Object(value)
    }

    pub(crate) const fn progress_observations(&self) -> u64 {
        self.progress_observations
    }

    pub(crate) fn since_last_progress_ms(&self, elapsed_ms: u64) -> u64 {
        elapsed_ms.saturating_sub(self.last_progress_elapsed_ms)
    }

    pub(crate) fn mark_quiet(&mut self) {
        if !matches!(
            self.phase,
            ObservationPhase::Terminal | ObservationPhase::Stalled
        ) {
            self.phase = ObservationPhase::Quiet;
        }
    }

    pub(crate) fn mark_stalled(&mut self) {
        self.phase = ObservationPhase::Stalled;
        self.stall_detected = true;
    }

    pub(crate) fn phase(&self) -> &'static str {
        self.phase.as_str()
    }

    pub(crate) fn last_active_phase(&self) -> &'static str {
        self.last_active_phase.as_str()
    }

    pub(crate) fn integrity_report(&self) -> Value {
        let channels = self
            .channel_stats
            .iter()
            .map(|(channel, stats)| (channel.clone(), stats.to_value()))
            .collect::<Map<_, _>>();
        let received = self
            .channel_stats
            .values()
            .map(|stats| stats.bytes_received)
            .sum::<u64>();
        let retained = serde_json::to_vec(&self.records)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default();
        let unchanged_spans = self
            .records
            .iter()
            .filter(|record| {
                record.get("kind").and_then(Value::as_str) == Some("observation_span")
                    && record.get("outcome").and_then(Value::as_str) == Some("unchanged")
            })
            .collect::<Vec<_>>();
        let longest_unchanged_polls = unchanged_spans
            .iter()
            .filter_map(|record| record.get("polls").and_then(Value::as_u64))
            .max()
            .unwrap_or_default();
        let longest_unchanged_ms = unchanged_spans
            .iter()
            .map(|record| {
                record
                    .get("last_elapsed_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                    .saturating_sub(
                        record
                            .get("first_elapsed_ms")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    )
            })
            .max()
            .unwrap_or_default();
        json!({
            "status": if self.violations.is_empty()
                && self.timeline_pages_drained
                && self.message_pages_drained
                && !self.stall_detected
            {
                "passed"
            } else {
                "failed"
            },
            "cursor_monotonic": self.violations.iter().all(|item| !item.contains("cursor")),
            "timeline_drained": self.timeline_pages_drained,
            "message_pages_drained": self.message_pages_drained,
            "omitted_changes": 0,
            "stall_detected": self.stall_detected,
            "phase": self.phase.as_str(),
            "last_active_phase": self.last_active_phase.as_str(),
            "progress_observations": self.progress_observations,
            "last_progress_elapsed_ms": self.last_progress_elapsed_ms,
            "message_count": self.messages.len(),
            "next_message_sequence": self.next_message_sequence,
            "timeline_event_count": self.timeline_events.len(),
            "timeline_pages": self.timeline_pages,
            "timeline_cursor_advances": self.timeline_cursor_advances,
            "timeline_cursor": self.timeline_cursor,
            "channels": channels,
            "bytes_received": received,
            "bytes_retained": retained,
            "retained_basis_points_of_received": if received == 0 {
                0
            } else {
                retained.saturating_mul(10_000).saturating_div(received)
            },
            "retained_record_count": self.records.len(),
            "longest_unchanged_polls": longest_unchanged_polls,
            "longest_unchanged_ms": longest_unchanged_ms,
            "violations": self.violations,
        })
    }

    #[cfg(test)]
    fn finish_trace(&mut self) -> Vec<Value> {
        let channels = self.pending_spans.keys().cloned().collect::<Vec<_>>();
        for channel in channels {
            self.flush_span(&channel);
        }
        self.records.sort_by_key(record_elapsed_ms);
        std::mem::take(&mut self.records)
    }

    pub(crate) fn finalize(&mut self) -> (Vec<Value>, Value) {
        let channels = self.pending_spans.keys().cloned().collect::<Vec<_>>();
        for channel in channels {
            self.flush_span(&channel);
        }
        self.records.sort_by_key(record_elapsed_ms);
        let report = self.integrity_report();
        (std::mem::take(&mut self.records), report)
    }

    pub(crate) fn fail_integrity<T>(&mut self, message: impl Into<String>) -> Result<T, String> {
        let message = message.into();
        self.violations.push(message.clone());
        Err(message)
    }

    fn update_phase_from_root(&mut self, summary: &Value) {
        let terminal = summary.get("terminal_state").and_then(Value::as_str);
        let live = summary.get("live_status").and_then(Value::as_str);
        self.phase = if terminal == Some("completed") {
            ObservationPhase::Terminal
        } else {
            match live {
                Some("preparing_context" | "queued" | "thinking") => ObservationPhase::Preparing,
                Some("calling_model") => ObservationPhase::CallingModel,
                Some("calling_tool") => ObservationPhase::CallingTool,
                Some("waiting_approval" | "paused") => ObservationPhase::WaitingHandoff,
                Some("finalizing") => ObservationPhase::Finalizing,
                Some("complete") => ObservationPhase::TerminalPending,
                Some("error" | "cancelled") => ObservationPhase::TerminalPending,
                _ => ObservationPhase::Bootstrapping,
            }
        };
        if !matches!(
            self.phase,
            ObservationPhase::Quiet
                | ObservationPhase::Stalled
                | ObservationPhase::Terminal
                | ObservationPhase::TerminalPending
        ) {
            self.last_active_phase = self.phase;
        }
    }

    fn mark_progress(&mut self, elapsed_ms: u64) {
        self.progress_observations = self.progress_observations.saturating_add(1);
        self.last_progress_elapsed_ms = elapsed_ms;
    }

    fn note_poll(&mut self, channel: &str, bytes: u64, error: bool) {
        let stats = self.channel_stats.entry(channel.to_string()).or_default();
        stats.polls = stats.polls.saturating_add(1);
        stats.bytes_received = stats.bytes_received.saturating_add(bytes);
        if error {
            stats.errors = stats.errors.saturating_add(1);
        }
    }

    fn record_changed(
        &mut self,
        channel: &str,
        path: &str,
        fingerprint: &str,
        elapsed_ms: u64,
        bytes_received: u64,
        response: Value,
    ) {
        self.flush_span(channel);
        let stats = self.channel_stats.entry(channel.to_string()).or_default();
        stats.changed = stats.changed.saturating_add(1);
        self.records.push(json!({
            "kind": "observation_transition",
            "channel": channel,
            "path": path,
            "outcome": "changed",
            "fingerprint": fingerprint,
            "elapsed_ms": elapsed_ms,
            "bytes_received": bytes_received,
            "response": response,
        }));
    }

    #[allow(clippy::too_many_arguments)]
    fn record_span(
        &mut self,
        channel: &str,
        path: &str,
        outcome: &str,
        fingerprint: &str,
        elapsed_ms: u64,
        bytes_received: u64,
        detail: Value,
    ) {
        if outcome == "unchanged" {
            let stats = self.channel_stats.entry(channel.to_string()).or_default();
            stats.unchanged = stats.unchanged.saturating_add(1);
        }
        let can_extend = self.pending_spans.get(channel).is_some_and(|span| {
            span.outcome == outcome && span.fingerprint == fingerprint && span.path == path
        });
        if can_extend {
            if let Some(span) = self.pending_spans.get_mut(channel) {
                span.last_elapsed_ms = elapsed_ms;
                span.polls = span.polls.saturating_add(1);
                span.bytes_received = span.bytes_received.saturating_add(bytes_received);
            }
            return;
        }
        self.flush_span(channel);
        self.pending_spans.insert(
            channel.to_string(),
            PendingSpan {
                channel: channel.to_string(),
                path: path.to_string(),
                outcome: outcome.to_string(),
                fingerprint: fingerprint.to_string(),
                first_elapsed_ms: elapsed_ms,
                last_elapsed_ms: elapsed_ms,
                polls: 1,
                bytes_received,
                detail,
            },
        );
    }

    fn flush_span(&mut self, channel: &str) {
        if let Some(span) = self.pending_spans.remove(channel) {
            self.records.push(span.to_value());
        }
    }

    fn integrity_error<T>(&mut self, message: &str) -> Result<T, String> {
        self.violations.push(message.to_string());
        Err(message.to_string())
    }

    fn record_integrity_error(&mut self, message: String) -> String {
        self.violations.push(message.clone());
        message
    }
}

fn response_bytes(response: &Result<Value, String>) -> u64 {
    match response {
        Ok(value) => serde_json::to_vec(value)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default(),
        Err(error) => error.len() as u64,
    }
}

fn timeline_event_identity(event: &Value) -> Result<String, String> {
    let source = event
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| "timeline event lacks source".to_string())?;
    let sequence = event
        .get("sequence")
        .and_then(Value::as_u64)
        .ok_or_else(|| "timeline event lacks sequence".to_string())?;
    let kind = event
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| "timeline event lacks kind".to_string())?;
    let stream = event
        .get("stream_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let commit = event
        .get("commit_cursor")
        .and_then(Value::as_u64)
        .map_or_else(|| "-".to_string(), |value| value.to_string());
    let transaction = event
        .get("transaction_index")
        .and_then(Value::as_u64)
        .map_or_else(|| "-".to_string(), |value| value.to_string());
    Ok(format!(
        "{source}:{stream}:{sequence}:{kind}:{commit}:{transaction}"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimelineCursorPosition {
    session: u64,
    runtime: Option<(u64, u64)>,
}

impl TimelineCursorPosition {
    fn strictly_advances_from(self, current: Self) -> bool {
        if self.session < current.session {
            return false;
        }
        let runtime_monotonic = match (current.runtime, self.runtime) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(current), Some(next)) => next >= current,
        };
        runtime_monotonic && (self.session > current.session || self.runtime != current.runtime)
    }
}

fn parse_timeline_cursor(value: &str) -> Result<TimelineCursorPosition, String> {
    let mut parts = value.split(':');
    let version = parts.next().unwrap_or_default();
    let session = parts
        .next()
        .ok_or_else(|| "timeline cursor lacks session position".to_string())?
        .parse::<u64>()
        .map_err(|_| "timeline cursor has invalid session position".to_string())?;
    let commit = parts
        .next()
        .ok_or_else(|| "timeline cursor lacks runtime commit position".to_string())?;
    let transaction = parts
        .next()
        .ok_or_else(|| "timeline cursor lacks runtime transaction position".to_string())?;
    if version != "v2" || parts.next().is_some() {
        return Err("timeline cursor has unsupported shape or version".to_string());
    }
    let runtime = match (commit, transaction) {
        ("-", "-") => None,
        ("-", _) | (_, "-") => {
            return Err("timeline cursor has an incomplete runtime position".to_string())
        }
        (commit, transaction) => Some((
            commit
                .parse::<u64>()
                .map_err(|_| "timeline cursor has invalid runtime commit".to_string())?,
            transaction
                .parse::<u64>()
                .map_err(|_| "timeline cursor has invalid runtime transaction".to_string())?,
        )),
    };
    Ok(TimelineCursorPosition { session, runtime })
}

fn timeline_event_order(event: &Value) -> (u64, String, u64, u64, u64) {
    (
        event
            .get("created_at_ms")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        event
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        event
            .get("commit_cursor")
            .and_then(Value::as_u64)
            .or_else(|| event.get("sequence").and_then(Value::as_u64))
            .unwrap_or_default(),
        event
            .get("transaction_index")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        event
            .get("sequence")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    )
}

fn record_elapsed_ms(value: &Value) -> u64 {
    value
        .get("elapsed_ms")
        .or_else(|| value.get("first_elapsed_ms"))
        .and_then(Value::as_u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(sequence: u64) -> Value {
        json!({
            "source": "runtime_lifecycle",
            "stream_id": "session:test",
            "sequence": sequence,
            "kind": "runtime.progress",
            "commit_cursor": sequence + 1,
            "transaction_index": 0,
            "status": "running",
        })
    }

    fn timeline_page(start: u64, count: u64, has_more: bool, cursor: &str) -> Value {
        json!({
            "events": (start..start + count).map(event).collect::<Vec<_>>(),
            "has_more": has_more,
            "next_cursor": cursor,
            "degraded": false,
        })
    }

    #[test]
    fn timeline_accumulates_more_than_one_gateway_page_without_loss() {
        let mut observer = LiveScenarioObserver::default();
        let first = observer
            .observe_timeline_page(
                "/timeline",
                &Ok(timeline_page(0, 500, true, "v2:1:500:0")),
                10,
            )
            .expect("first page");
        assert!(first.has_more);
        let second = observer
            .observe_timeline_page(
                "/timeline?cursor=v2:1:500:0",
                &Ok(timeline_page(500, 128, false, "v2:1:628:0")),
                11,
            )
            .expect("second page");
        assert!(!second.has_more);
        assert_eq!(
            observer.timeline()["events"].as_array().map(Vec::len),
            Some(628)
        );
        assert_eq!(observer.integrity_report()["timeline_drained"], true);
        assert_eq!(observer.integrity_report()["timeline_cursor_advances"], 2);
    }

    #[test]
    fn timeline_rejects_cursor_cycles_and_empty_has_more_pages() {
        let mut observer = LiveScenarioObserver::default();
        observer
            .observe_timeline_page("/timeline", &Ok(timeline_page(0, 1, true, "v2:1:1:0")), 1)
            .expect("baseline");
        assert!(observer
            .observe_timeline_page("/timeline", &Ok(timeline_page(1, 1, true, "v2:1:1:0")), 2,)
            .is_err());

        let mut observer = LiveScenarioObserver::default();
        assert!(observer
            .observe_timeline_page(
                "/timeline",
                &Ok(json!({"events": [], "has_more": true, "next_cursor": "v2:0:0:0"})),
                1,
            )
            .is_err());
    }

    #[test]
    fn timeline_rejects_semantic_cursor_regression_and_identity_mutation() {
        let mut observer = LiveScenarioObserver::default();
        observer
            .observe_timeline_page("/timeline", &Ok(timeline_page(0, 1, false, "v2:2:5:1")), 1)
            .expect("baseline");
        assert!(observer
            .observe_timeline_page("/timeline", &Ok(timeline_page(1, 1, false, "v2:1:6:0")), 2,)
            .is_err());

        let mut observer = LiveScenarioObserver::default();
        observer
            .observe_timeline_page("/timeline", &Ok(timeline_page(0, 1, false, "v2:1:1:0")), 1)
            .expect("baseline");
        let mut mutated = event(0);
        mutated["status"] = json!("completed");
        assert!(observer
            .observe_timeline_page(
                "/timeline",
                &Ok(json!({
                    "events": [mutated],
                    "has_more": false,
                    "next_cursor": "v2:1:2:0",
                })),
                2,
            )
            .is_err());
    }

    #[test]
    fn message_cursor_and_stable_identity_fail_closed() {
        let mut observer = LiveScenarioObserver::default();
        assert!(observer
            .observe_message_page(
                "/messages?from_seq=0",
                &Ok(json!({
                    "messages": [{"id":"input","sequence":0,"role":"user"}],
                    "next_seq": 2,
                    "has_more": false,
                })),
                1,
            )
            .is_err());

        let mut observer = LiveScenarioObserver::default();
        observer
            .observe_message_page(
                "/messages?from_seq=0",
                &Ok(json!({
                    "messages": [{"id":"input","sequence":0,"role":"user"}],
                    "next_seq": 1,
                    "has_more": false,
                })),
                1,
            )
            .expect("baseline");
        assert!(observer
            .observe_message_page(
                "/messages?from_seq=1",
                &Ok(json!({
                    "messages": [{"id":"input","sequence":1,"role":"assistant"}],
                    "next_seq": 2,
                    "has_more": false,
                })),
                2,
            )
            .is_err());
    }

    #[test]
    fn transient_errors_require_a_successful_final_drain() {
        let mut observer = LiveScenarioObserver::default();
        observer
            .observe_message_page("/messages", &Err("temporary".to_string()), 1)
            .expect("transient message error is retryable");
        observer
            .observe_timeline_page("/timeline", &Err("temporary".to_string()), 1)
            .expect("transient timeline error is retryable");
        assert_eq!(observer.integrity_report()["status"], "failed");

        observer
            .observe_message_page(
                "/messages",
                &Ok(json!({"messages": [], "next_seq": null, "has_more": false})),
                2,
            )
            .expect("message recovery");
        observer
            .observe_timeline_page(
                "/timeline",
                &Ok(json!({"events": [], "next_cursor": null, "has_more": false})),
                2,
            )
            .expect("timeline recovery");
        assert_eq!(observer.integrity_report()["status"], "passed");
    }

    #[test]
    fn incremental_messages_do_not_retain_the_original_prompt_repeatedly() {
        let mut observer = LiveScenarioObserver::default();
        let prompt = "x".repeat(8_000);
        observer
            .observe_message_page(
                "/messages?from_seq=0",
                &Ok(json!({
                    "messages": [{"id":"input","sequence":0,"role":"user","text":prompt}],
                    "next_seq": 1,
                    "has_more": false,
                })),
                0,
            )
            .expect("baseline");
        for elapsed in 1..=2_558 {
            observer
                .observe_message_page(
                    "/messages?from_seq=1",
                    &Ok(json!({"messages":[],"next_seq":null,"has_more":false})),
                    elapsed,
                )
                .expect("empty delta");
        }
        observer
            .observe_message_page(
                "/messages?from_seq=1",
                &Ok(json!({
                    "messages": [{"id":"answer","sequence":1,"role":"assistant","text":"done"}],
                    "next_seq": 2,
                    "has_more": false,
                })),
                2_559,
            )
            .expect("answer");
        let trace = observer.finish_trace();
        let retained_bytes = serde_json::to_vec(&trace).expect("serialize trace").len();
        let old_repeated_prompt_bytes = 8_000_usize * 2_558;
        let unchanged_polls = trace
            .iter()
            .filter(|record| record["channel"] == "messages")
            .filter_map(|record| record.get("polls").and_then(Value::as_u64))
            .sum::<u64>();
        assert_eq!(unchanged_polls, 2_558);
        assert!(trace.len() < 10, "unchanged polls must form bounded spans");
        assert!(
            retained_bytes.saturating_mul(10) < old_repeated_prompt_bytes,
            "retained polling evidence must be at least 90% smaller than full-page replay"
        );
        assert_eq!(observer.assistant_message().unwrap()["id"], "answer");
    }

    #[test]
    fn every_root_live_change_is_retained_while_unchanged_polls_are_spans() {
        let mut observer = LiveScenarioObserver::default();
        for revision in 1..=295 {
            let summary = json!({
                "terminal_state": "pending",
                "live_status": "calling_model",
                "live_revision": revision,
                "live_output_bytes": revision * 10,
                "live_last_progress_at_ms": revision * 100,
            });
            let fingerprint = format!("revision={revision}");
            observer.observe_root(
                "/execution",
                &Ok(summary.clone()),
                Some(&fingerprint),
                None,
                revision,
            );
            observer.observe_root(
                "/execution",
                &Ok(summary),
                Some(&fingerprint),
                None,
                revision + 1,
            );
        }
        let trace = observer.finish_trace();
        let changes = trace
            .iter()
            .filter(|record| {
                record["channel"] == "root" && record["kind"] == "observation_transition"
            })
            .count();
        assert_eq!(changes, 295);
        assert_eq!(observer.progress_observations(), 294);
    }

    #[test]
    fn lightweight_live_channel_preserves_each_sampled_revision() {
        let mut observer = LiveScenarioObserver::default();
        for revision in 1..=32 {
            let summary = json!({
                "execution_id": "root",
                "live_revision": revision,
                "live_status": "calling_model",
                "live_output_bytes": revision * 100,
                "live_last_progress_at_ms": revision * 500,
            });
            observer.observe_live(
                "/session/execution/live",
                &Ok(summary),
                Some(&format!("live={revision}")),
                Some(256),
                revision * 500,
            );
        }
        let (trace, report) = observer.finalize();
        assert_eq!(report["channels"]["live"]["changed"], 32);
        assert_eq!(report["channels"]["live"]["bytes_received"], 8_192);
        assert_eq!(
            trace
                .iter()
                .filter(|record| record["channel"] == "live")
                .count(),
            32
        );
    }

    #[test]
    fn phase_classification_distinguishes_active_quiet_and_stalled_work() {
        let mut observer = LiveScenarioObserver::default();
        let summary = json!({
            "terminal_state": "pending",
            "live_status": "calling_tool",
            "live_revision": 1,
        });
        observer.observe_root("/execution", &Ok(summary), Some("one"), None, 1);
        assert_eq!(observer.phase(), "calling_tool");
        observer.mark_quiet();
        assert_eq!(observer.phase(), "quiet");
        assert_eq!(observer.last_active_phase(), "calling_tool");
        observer.mark_stalled();
        assert_eq!(observer.phase(), "stalled");
        assert_eq!(observer.integrity_report()["stall_detected"], true);
    }

    #[test]
    fn bounded_page_drain_exhaustion_is_an_integrity_failure() {
        let mut observer = LiveScenarioObserver::default();
        let result: Result<(), String> = observer.fail_integrity(format!(
            "timeline pagination exceeded {} pages without draining",
            MAX_DRAIN_PAGES_PER_PROBE
        ));
        assert!(result.is_err());
        assert_eq!(observer.integrity_report()["status"], "failed");
        assert_eq!(
            observer.integrity_report()["violations"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }
}
