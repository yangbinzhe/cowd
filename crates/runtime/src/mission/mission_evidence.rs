//! Replayable Mission evidence query projection.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};

use crate::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeTransactionEventInput,
};

pub(crate) const MISSION_EVIDENCE_KIND: &str = "mission_evidence.recorded.v1";
const PROJECTOR_STREAM: &str = "mission-evidence-projector";
const PROJECTOR_ID: &str = "projector:mission-evidence";
const DLQ_KIND: &str = "mission_evidence.projector.failed.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionEvidenceRef {
    pub evidence: EvidenceRef,
    pub mission_id: Option<String>,
    pub session_id: String,
    pub team_id: Option<String>,
    pub agent_id: Option<String>,
    pub kind: String,
    pub summary: String,
    pub source_ref: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct MissionEvidenceProjection {
    source_cursor: u64,
    revision: u64,
    projected_at_ms: u64,
    records: BTreeMap<String, MissionEvidenceRef>,
    dlq_count: u64,
}

#[derive(Debug)]
pub struct MissionEvidenceBus {
    projection: RwLock<MissionEvidenceProjection>,
    projection_lock: Mutex<()>,
    event_store: Arc<RuntimeEventStore>,
}

impl MissionEvidenceBus {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        let projection = restore_projection(&event_store)
            .and_then(|projection| replay_projection(&event_store, projection))
            .unwrap_or_default();
        Self {
            projection: RwLock::new(projection),
            projection_lock: Mutex::new(()),
            event_store,
        }
    }

    pub fn record(&self, evidence: MissionEvidenceRef) -> Result<MissionEvidenceRef, String> {
        let evidence = normalize_evidence(evidence);
        let event = evidence_event(&evidence)?;
        let stream_id = event.stream_id.clone();
        let key = format!("mission-evidence:{}", evidence.evidence.id);
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, &key)
            .map_err(|error| error.to_string())?
            .is_none()
        {
            let revision = self
                .event_store
                .stream_revision(&stream_id)
                .map_err(|error| error.to_string())?;
            if let Err(error) = self.event_store.append_batch_if_revision(
                stream_id.clone(),
                revision,
                key.clone(),
                vec![RuntimeTransactionEventInput {
                    event,
                    idempotency_key: Some(key.clone()),
                    schema_version: 1,
                }],
            ) {
                let committed = self
                    .event_store
                    .event_by_idempotency_key(&stream_id, &key)
                    .map_err(|lookup_error| lookup_error.to_string())?
                    .is_some();
                if !committed {
                    return Err(error.to_string());
                }
            }
        }
        self.project_available(128)?;
        Ok(evidence)
    }

    pub(crate) fn record_with_related_event(
        &self,
        evidence: MissionEvidenceRef,
        related_event: RuntimeTransactionEventInput,
        transaction_id: String,
    ) -> Result<MissionEvidenceRef, String> {
        let evidence = normalize_evidence(evidence);
        let key = format!("mission-evidence:{}", evidence.evidence.id);
        let evidence_event = evidence_event(&evidence)?;
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: evidence_event.stream_id.clone(),
            expected_revision: self
                .event_store
                .stream_revision(&evidence_event.stream_id)
                .map_err(|error| error.to_string())?,
        }];
        if related_event.event.stream_id != evidence_event.stream_id {
            expected_streams.push(ExpectedStreamRevision {
                stream_id: related_event.event.stream_id.clone(),
                expected_revision: self
                    .event_store
                    .stream_revision(&related_event.event.stream_id)
                    .map_err(|error| error.to_string())?,
            });
        }
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id,
                expected_streams,
                events: vec![
                    RuntimeTransactionEventInput {
                        event: evidence_event,
                        idempotency_key: Some(key),
                        schema_version: 1,
                    },
                    related_event,
                ],
            })
            .map_err(|error| error.to_string())?;
        self.project_available(128)?;
        Ok(evidence)
    }

    pub fn project_available(&self, max_commits: usize) -> Result<usize, String> {
        let _projection_guard = self
            .projection_lock
            .lock()
            .map_err(|_| "mission evidence projection lock poisoned".to_string())?;
        let current = self
            .projection
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut next = current.clone();
        let batches = self
            .event_store
            .events_after_cursor(current.source_cursor, max_commits.max(1))
            .map_err(|error| error.to_string())?;
        if batches.is_empty() {
            return Ok(0);
        }
        let mut processed = 0;
        let mut source_changed = false;
        for batch in batches {
            for event in &batch.events {
                if event.stream_id == PROJECTOR_STREAM {
                    continue;
                }
                if event.kind != MISSION_EVIDENCE_KIND {
                    continue;
                }
                source_changed = true;
                match serde_json::from_value::<MissionEvidenceRef>(event.payload.clone()) {
                    Ok(evidence) => {
                        next.records.insert(evidence.evidence.id.clone(), evidence);
                    }
                    Err(error) => {
                        next.dlq_count = next.dlq_count.saturating_add(1);
                        record_dlq(
                            &self.event_store,
                            &event.event_id,
                            batch.commit_cursor,
                            &error.to_string(),
                        )?;
                        tracing::warn!(
                            event_id = event.event_id,
                            %error,
                            "mission evidence projection rejected an event"
                        );
                    }
                }
            }
            next.source_cursor = batch.commit_cursor;
            processed += 1;
        }
        if !source_changed {
            checkpoint(&self.event_store, &next)?;
            *self
                .projection
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
            return Ok(processed);
        }
        next.revision = next.revision.saturating_add(1);
        next.projected_at_ms = now_ms();
        checkpoint(&self.event_store, &next)?;
        *self
            .projection
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        Ok(processed)
    }

    #[must_use]
    pub fn list_for_session(&self, session_id: &str) -> Vec<MissionEvidenceRef> {
        filtered_records(&self.projection, |item| item.session_id == session_id)
    }

    #[must_use]
    pub fn list_for_team(&self, team_id: &str) -> Vec<MissionEvidenceRef> {
        filtered_records(&self.projection, |item| {
            item.team_id.as_deref() == Some(team_id)
        })
    }

    #[must_use]
    pub fn list_all(&self) -> Vec<MissionEvidenceRef> {
        filtered_records(&self.projection, |_| true)
    }

    #[must_use]
    pub fn projection(&self) -> serde_json::Value {
        let projection = self
            .projection
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut evidence = projection.records.values().cloned().collect::<Vec<_>>();
        evidence.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        let (latest_source_cursor, lag_commits) =
            mission_evidence_source_lag(&self.event_store, projection.source_cursor)
                .unwrap_or((projection.source_cursor, 0));
        serde_json::json!({
            "kind": "runtime.mission_evidence",
            "count": projection.records.len(),
            "revision": projection.revision,
            "source_cursor": projection.source_cursor,
            "projected_at_ms": projection.projected_at_ms,
            "latest_source_cursor": latest_source_cursor,
            "lag_commits": lag_commits,
            "freshness_ms": now_ms().saturating_sub(projection.projected_at_ms),
            "dlq_count": projection.dlq_count,
            "latest": evidence.into_iter().take(100).collect::<Vec<_>>(),
        })
    }
}

fn filtered_records(
    projection: &RwLock<MissionEvidenceProjection>,
    predicate: impl Fn(&MissionEvidenceRef) -> bool,
) -> Vec<MissionEvidenceRef> {
    let projection = projection
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut records = projection
        .records
        .values()
        .filter(|record| predicate(record))
        .cloned()
        .collect::<Vec<_>>();
    records.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
    records
}

fn normalize_evidence(mut evidence: MissionEvidenceRef) -> MissionEvidenceRef {
    if evidence.evidence.id.trim().is_empty() {
        evidence.evidence.id = format!("mission-evidence-{}", uuid::Uuid::new_v4());
    }
    if evidence.created_at_ms == 0 {
        evidence.created_at_ms = now_ms();
    }
    evidence
}

fn evidence_event(evidence: &MissionEvidenceRef) -> Result<RuntimeEventInput, String> {
    Ok(RuntimeEventInput {
        stream_id: evidence
            .team_id
            .as_ref()
            .map(|team_id| format!("team:{team_id}"))
            .unwrap_or_else(|| format!("session:{}", evidence.session_id)),
        scope: RuntimeEventScope::Mission,
        kind: MISSION_EVIDENCE_KIND.to_string(),
        status: Some("recorded".to_string()),
        actor: Some("runtime.mission_evidence_projector".to_string()),
        refs: vec![RuntimeEventRef {
            kind: "evidence".to_string(),
            id: evidence.evidence.id.clone(),
        }],
        payload: serde_json::to_value(evidence).map_err(|error| error.to_string())?,
    })
}

fn restore_projection(
    event_store: &RuntimeEventStore,
) -> Result<MissionEvidenceProjection, String> {
    let Some(checkpoint) = event_store
        .projection_checkpoint(PROJECTOR_ID)
        .map_err(|error| error.to_string())?
    else {
        return Ok(MissionEvidenceProjection::default());
    };
    let payload = checkpoint
        .payload
        .get("projection")
        .cloned()
        .unwrap_or(checkpoint.payload);
    let mut projection = serde_json::from_value::<MissionEvidenceProjection>(payload)
        .map_err(|error| error.to_string())?;
    projection.source_cursor = checkpoint.source_cursor;
    Ok(projection)
}

fn replay_projection(
    event_store: &RuntimeEventStore,
    mut projection: MissionEvidenceProjection,
) -> Result<MissionEvidenceProjection, String> {
    let upper_bound = *event_store.subscribe_commits().borrow();
    let events =
        event_store.replay_scope_kind(RuntimeEventScope::Mission, MISSION_EVIDENCE_KIND)?;
    for event in events.into_iter().filter(|event| {
        event.commit_cursor > projection.source_cursor && event.commit_cursor <= upper_bound
    }) {
        match serde_json::from_value::<MissionEvidenceRef>(event.payload.clone()) {
            Ok(evidence) => {
                projection
                    .records
                    .insert(evidence.evidence.id.clone(), evidence);
            }
            Err(error) => {
                projection.dlq_count = projection.dlq_count.saturating_add(1);
                record_dlq(
                    event_store,
                    &event.event_id,
                    event.commit_cursor,
                    &error.to_string(),
                )?;
            }
        }
    }
    projection.source_cursor = upper_bound;
    Ok(projection)
}

fn mission_evidence_events_after(
    event_store: &RuntimeEventStore,
    source_cursor: u64,
) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
    let mut events = Vec::new();
    let mut after_position = Some((source_cursor, u32::MAX));
    loop {
        let page = event_store.list_scope_kind_page_asc(
            RuntimeEventScope::Mission,
            MISSION_EVIDENCE_KIND,
            after_position,
            128,
        )?;
        if page.is_empty() {
            break;
        }
        let complete = page.len() < 128;
        after_position = page
            .last()
            .map(|event| (event.commit_cursor, event.transaction_index));
        events.extend(page);
        if complete {
            break;
        }
    }
    Ok(events)
}

fn mission_evidence_source_lag(
    event_store: &RuntimeEventStore,
    source_cursor: u64,
) -> Result<(u64, u64), String> {
    let events = mission_evidence_events_after(event_store, source_cursor)?;
    Ok((
        events
            .last()
            .map_or(source_cursor, |event| event.commit_cursor),
        events.len() as u64,
    ))
}

fn record_dlq(
    event_store: &RuntimeEventStore,
    source_event_id: &str,
    source_cursor: u64,
    error: &str,
) -> Result<(), String> {
    let key = format!("dead-letter:{source_event_id}");
    if event_store
        .event_by_idempotency_key(PROJECTOR_STREAM, &key)
        .map_err(|lookup_error| lookup_error.to_string())?
        .is_some()
    {
        return Ok(());
    }
    event_store
        .append_batch_if_revision(
            PROJECTOR_STREAM,
            event_store
                .stream_revision(PROJECTOR_STREAM)
                .map_err(|revision_error| revision_error.to_string())?,
            format!("mission-evidence-dlq:{source_event_id}"),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: PROJECTOR_STREAM.to_string(),
                    scope: RuntimeEventScope::Recovery,
                    kind: DLQ_KIND.to_string(),
                    status: Some("blocked".to_string()),
                    actor: Some("runtime.mission_evidence_projector".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "source_event".to_string(),
                        id: source_event_id.to_string(),
                    }],
                    payload: serde_json::json!({
                        "source_event_id": source_event_id,
                        "source_cursor": source_cursor,
                        "error": error,
                    }),
                },
                idempotency_key: Some(key),
                schema_version: 1,
            }],
        )
        .map_err(|append_error| append_error.to_string())?;
    Ok(())
}

fn checkpoint(
    event_store: &RuntimeEventStore,
    projection: &MissionEvidenceProjection,
) -> Result<(), String> {
    event_store
        .put_projection_checkpoint(
            PROJECTOR_ID,
            projection.source_cursor,
            &serde_json::to_value(projection).map_err(|error| error.to_string())?,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(id: &str) -> MissionEvidenceRef {
        MissionEvidenceRef {
            evidence: EvidenceRef::observed("mission_test", id).with_source("runtime.test"),
            mission_id: Some("mission-1".to_string()),
            session_id: "session-1".to_string(),
            team_id: Some("team-1".to_string()),
            agent_id: None,
            kind: "test".to_string(),
            summary: "checked".to_string(),
            source_ref: None,
            created_at_ms: 1,
        }
    }

    #[test]
    fn projection_recovers_and_duplicate_record_is_idempotent() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let bus = MissionEvidenceBus::new(Arc::clone(&store));
        bus.record(evidence("e1")).unwrap();
        bus.record(evidence("e1")).unwrap();
        assert_eq!(bus.list_all().len(), 1);

        let restarted = MissionEvidenceBus::new(store);
        assert_eq!(restarted.list_all(), bus.list_all());
        assert!(restarted
            .event_store
            .replay_scope_kind(
                RuntimeEventScope::Recovery,
                "mission_evidence.projector.checkpoint.v1"
            )
            .unwrap()
            .is_empty());
        assert!(restarted
            .event_store
            .projection_checkpoint(PROJECTOR_ID)
            .unwrap()
            .is_some());
    }

    #[test]
    fn malformed_source_is_invisible_and_restarts_from_durable_dlq_state() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        store
            .append(RuntimeEventInput {
                stream_id: "session:session-1".to_string(),
                scope: RuntimeEventScope::Mission,
                kind: MISSION_EVIDENCE_KIND.to_string(),
                status: Some("recorded".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"not": "mission evidence"}),
            })
            .unwrap();

        let bus = MissionEvidenceBus::new(Arc::clone(&store));
        assert!(bus.list_all().is_empty());
        assert_eq!(bus.projection()["dlq_count"], 1);

        let restarted = MissionEvidenceBus::new(store);
        assert!(restarted.list_all().is_empty());
        assert_eq!(restarted.projection()["dlq_count"], 1);
    }
}
