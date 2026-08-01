use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use chrono::{Duration as ChronoDuration, Utc};
use harness_contract::managed_agent::ManagedAgentTriggerEvent;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use surface::{
    normalize_surface_id, SurfaceFrame, SurfaceMessageLedger, SurfaceOperationResult,
    SurfaceSendRequest,
};

pub(crate) use surface::{
    SurfaceDeliveryEvent, SurfaceInboxReceipt, SurfaceInboxRecord, SurfaceIngressClaim,
    SurfaceIngressFrameRecord, SurfaceMessageLedgerMigrationSnapshot, SurfaceMessageSnapshot,
    SurfaceOutboxRecord, SurfaceSessionProjectionDraft, SurfaceTriggerEventReceipt,
    SurfaceTriggerEventRecord, SurfaceTurnCorrelation,
};

const INBOX_FILE: &str = "surface_inbox.jsonl";
const OUTBOX_FILE: &str = "surface_outbox.jsonl";
const EVENT_FILE: &str = "surface_delivery_event.jsonl";
const TRIGGER_EVENT_FILE: &str = "surface_trigger_event.jsonl";
const DATABASE_FILE: &str = "surface_messages.sqlite3";
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Default)]
struct SurfaceMessageState {
    inbox: BTreeMap<String, SurfaceInboxRecord>,
    outbox: BTreeMap<String, SurfaceOutboxRecord>,
    trigger_events: BTreeMap<String, SurfaceTriggerEventRecord>,
    events: BTreeMap<String, SurfaceDeliveryEvent>,
}

#[derive(Debug, Clone)]
pub(crate) struct SqliteSurfaceMessageStore {
    root: PathBuf,
    executor: storage::SqliteExecutor,
}

impl SqliteSurfaceMessageStore {
    #[cfg(test)]
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self::try_new(root).expect("isolated Surface test store")
    }

    pub(crate) fn try_new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        Self::open_at(root.clone(), root.join(DATABASE_FILE))
    }

    /// Production composition receives the resolved endpoint from `storage`.
    /// The `try_new(root)` helper remains for isolated stores and intentionally
    /// does not participate in Gateway's durable-store bootstrap.
    pub(crate) fn from_storage_endpoint(
        endpoint: &storage::StorageEndpoint,
    ) -> Result<Self, String> {
        if endpoint.backend != storage::StorageBackendKind::Sqlite {
            return Err("surface messages require a sqlite endpoint".to_string());
        }
        let database_path = endpoint.as_handle().path;
        let root = database_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let executor =
            storage::SqliteExecutor::for_endpoint(endpoint).map_err(|error| error.to_string())?;
        Self::open_with_executor(root, executor)
    }

    pub(crate) fn in_memory(identity: impl Into<String>) -> Result<Self, String> {
        let identity = identity.into();
        let executor = storage::SqliteExecutor::in_memory(identity.clone())
            .map_err(|error| error.to_string())?;
        Self::open_with_executor(PathBuf::from(format!("memory://{identity}")), executor)
    }

    fn open_at(root: PathBuf, database_path: PathBuf) -> Result<Self, String> {
        let handle = storage::StorageHandle::sqlite(
            "surface_messages",
            database_path,
            "surface",
            "surface_message_executor",
        );
        let executor =
            storage::SqliteExecutor::for_handle(&handle).map_err(|error| error.to_string())?;
        Self::open_with_executor(root, executor)
    }

    fn open_with_executor(
        root: PathBuf,
        executor: storage::SqliteExecutor,
    ) -> Result<Self, String> {
        let connection = executor.checkout().map_err(|error| error.to_string())?;
        initialize_database(&connection)?;
        drop(connection);
        let store = Self { root, executor };
        store.import_legacy_jsonl_once()?;
        store.reconcile_after_restart()?;
        Ok(store)
    }

    pub(crate) fn default_root(config_home: &Path) -> PathBuf {
        std::env::var_os("COWD_SURFACE_MESSAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| config_home.join("surface-messages"))
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Stop-time reverse export for operator rollback. Production mutations
    /// never call this path and never dual-write JSONL.
    #[allow(dead_code)]
    pub(crate) fn export_legacy_jsonl(&self, target_root: &Path) -> Result<(), String> {
        let state = self.lock_state()?;
        fs::create_dir_all(target_root).map_err(|error| error.to_string())?;
        write_jsonl_atomic(target_root.join(INBOX_FILE), state.inbox.values())?;
        write_jsonl_atomic(target_root.join(OUTBOX_FILE), state.outbox.values())?;
        write_jsonl_atomic(
            target_root.join(TRIGGER_EVENT_FILE),
            state.trigger_events.values(),
        )?;
        write_jsonl_atomic(target_root.join(EVENT_FILE), state.events.values())
    }

    pub(crate) fn persist_ingress_frame(&self, frame: &SurfaceFrame) -> Result<String, String> {
        let SurfaceFrame::Event {
            surface, payload, ..
        } = frame
        else {
            return Err("only Surface event frames can enter the durable ingress".to_string());
        };
        let frame_json = serde_json::to_value(frame).map_err(|error| error.to_string())?;
        let record_key = format!("surface-ingress:{}", hash_json(&frame_json));
        let session_id = super::ingress::surface_session_id(surface, payload);
        let now = now_ms();
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT OR IGNORE INTO surface_ingress_frame(
                   record_key, surface, session_id, status, attempts, max_attempts,
                   next_retry_at_ms, created_at_ms, updated_at_ms, payload_json
                 ) VALUES(?1, ?2, ?3, 'pending', 0, ?4, ?5, ?5, ?5, ?6)",
                params![
                    record_key,
                    normalize_surface_id(surface),
                    session_id,
                    DEFAULT_MAX_ATTEMPTS,
                    now,
                    frame_json.to_string(),
                ],
            )
            .map_err(|error| error.to_string())?;
        Ok(record_key)
    }

    pub(crate) fn claim_ingress_frames(
        &self,
        claim_owner: &str,
        limit: usize,
        lease_ms: i64,
    ) -> Result<Vec<SurfaceIngressClaim>, String> {
        let now = now_ms();
        let lease_until = now.saturating_add(lease_ms.max(1));
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "UPDATE surface_ingress_frame
                 SET status='retry_scheduled', claim_owner=NULL, lease_until_ms=NULL,
                     next_retry_at_ms=?1, updated_at_ms=?1,
                     last_error='gateway worker lease expired before durable completion'
                 WHERE status='claimed' AND lease_until_ms <= ?1",
                params![now],
            )
            .map_err(|error| error.to_string())?;
        let mut active_sessions = std::collections::BTreeSet::new();
        {
            let mut active = transaction
                .prepare(
                    "SELECT DISTINCT session_id FROM surface_ingress_frame
                     WHERE status='claimed' AND lease_until_ms > ?1",
                )
                .map_err(|error| error.to_string())?;
            let rows = active
                .query_map(params![now], |row| row.get::<_, String>(0))
                .map_err(|error| error.to_string())?;
            for row in rows {
                active_sessions.insert(row.map_err(|error| error.to_string())?);
            }
        }
        let candidates = {
            let mut statement = transaction
                .prepare(
                    "SELECT record_key, session_id, payload_json
                     FROM surface_ingress_frame
                     WHERE status IN ('pending', 'retry_scheduled')
                       AND attempts < max_attempts
                       AND (next_retry_at_ms IS NULL OR next_retry_at_ms <= ?1)
                     ORDER BY created_at_ms, record_key
                     LIMIT ?2",
                )
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map(
                    params![now, limit.saturating_mul(8).max(limit) as i64],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .map_err(|error| error.to_string())?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?
        };
        let mut claims = Vec::new();
        for (record_key, session_id, payload_json) in candidates {
            if claims.len() >= limit || !active_sessions.insert(session_id) {
                continue;
            }
            let changed = transaction
                .execute(
                    "UPDATE surface_ingress_frame
                     SET status='claimed', attempts=attempts+1, claim_owner=?2,
                         lease_until_ms=?3, next_retry_at_ms=NULL, updated_at_ms=?1,
                         last_error=NULL
                     WHERE record_key=?4 AND status IN ('pending', 'retry_scheduled')",
                    params![now, claim_owner, lease_until, record_key],
                )
                .map_err(|error| error.to_string())?;
            if changed == 0 {
                continue;
            }
            claims.push(SurfaceIngressClaim {
                record_key,
                frame: serde_json::from_str(&payload_json).map_err(|error| error.to_string())?,
            });
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(claims)
    }

    pub(crate) fn complete_ingress_frame(&self, record_key: &str) -> Result<(), String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        connection
            .execute(
                "UPDATE surface_ingress_frame
                 SET status='completed', claim_owner=NULL, lease_until_ms=NULL,
                     next_retry_at_ms=NULL, updated_at_ms=?1, last_error=NULL
                 WHERE record_key=?2 AND status='claimed'",
                params![now_ms(), record_key],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub(crate) fn fail_ingress_frame(&self, record_key: &str, error: &str) -> Result<(), String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let (attempts, max_attempts) = connection
            .query_row(
                "SELECT attempts, max_attempts FROM surface_ingress_frame WHERE record_key=?1",
                params![record_key],
                |row| Ok((row.get::<_, u32>(0)?, row.get::<_, u32>(1)?)),
            )
            .map_err(|db_error| db_error.to_string())?;
        let terminal = attempts >= max_attempts;
        connection
            .execute(
                "UPDATE surface_ingress_frame
                 SET status=?1, claim_owner=NULL, lease_until_ms=NULL,
                     next_retry_at_ms=?2, updated_at_ms=?3, last_error=?4
                 WHERE record_key=?5",
                params![
                    if terminal {
                        "dead_letter"
                    } else {
                        "retry_scheduled"
                    },
                    (!terminal).then(|| next_retry_at_ms(attempts)),
                    now_ms(),
                    error,
                    record_key,
                ],
            )
            .map_err(|db_error| db_error.to_string())?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn ingress_frame_count(&self) -> usize {
        self.executor
            .checkout()
            .map_err(|error| error.to_string())
            .ok()
            .and_then(|connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM surface_ingress_frame", [], |row| {
                        row.get::<_, usize>(0)
                    })
                    .ok()
            })
            .unwrap_or(0)
    }

    pub(crate) fn record_inbox_received(
        &self,
        surface: &str,
        message_id: &str,
        payload: &serde_json::Value,
        runtime_session_id: &str,
        thread_id: Option<String>,
        sender_id: Option<String>,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<SurfaceInboxReceipt, String> {
        let surface = normalize_surface_id(surface);
        let idempotency_key = inbound_idempotency_key(&surface, message_id);
        let mut state = self.lock_state()?;
        if state.inbox.contains_key(&idempotency_key) {
            drop(state);
            self.stage_inbox_projections(&idempotency_key, projections)?;
            let existing = self
                .lock_state()?
                .inbox
                .get(&idempotency_key)
                .cloned()
                .ok_or_else(|| "surface inbox disappeared during duplicate repair".to_string())?;
            return Ok(SurfaceInboxReceipt {
                record: existing,
                duplicate: true,
            });
        }
        let now = now_ms();
        let mut record = SurfaceInboxRecord {
            id: idempotency_key.clone(),
            surface: surface.clone(),
            message_id: message_id.to_string(),
            idempotency_key: idempotency_key.clone(),
            thread_id,
            sender_id,
            payload_hash: hash_json(payload),
            payload_summary: summarize_json(payload, 240),
            payload_json: payload.clone(),
            status: "received".to_string(),
            received_at_ms: now,
            updated_at_ms: now,
            runtime_session_id: Some(runtime_session_id.to_string()),
            runtime_turn_id: None,
            correlation: None,
            session_projections: Vec::new(),
            last_error: None,
        };
        record.stage_session_projections(projections)?;
        state.inbox.insert(idempotency_key.clone(), record.clone());
        if !self.insert_record_if_absent(INBOX_FILE, &record)? {
            drop(state);
            self.stage_inbox_projections(&idempotency_key, projections)?;
            let existing = self
                .lock_state()?
                .inbox
                .get(&idempotency_key)
                .cloned()
                .ok_or_else(|| {
                    "surface inbox idempotency race lost without durable row".to_string()
                })?;
            return Ok(SurfaceInboxReceipt {
                record: existing,
                duplicate: true,
            });
        }
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface,
            delivery_id: None,
            message_id: Some(message_id.to_string()),
            kind: "inbox.received".to_string(),
            status: "received".to_string(),
            detail_json: serde_json::json!({
                "runtime_session_id": runtime_session_id,
                "payload_summary": record.payload_summary,
            }),
            created_at_ms: now,
        })?;
        Ok(SurfaceInboxReceipt {
            record,
            duplicate: false,
        })
    }

    pub(crate) fn mark_inbox_processing(
        &self,
        idempotency_key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        let (record, staged) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| -> Result<(), String> {
                record.stage_session_projections(projections)?;
                record.status = "processing".to_string();
                record.updated_at_ms = now_ms();
                record.last_error = None;
                Ok(())
            },
        )?;
        staged?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface,
            delivery_id: None,
            message_id: Some(record.message_id),
            kind: "inbox.processing".to_string(),
            status: "processing".to_string(),
            detail_json: serde_json::json!({
                "session_projection_count": record.session_projections.len(),
            }),
            created_at_ms: now_ms(),
        })?;
        Ok(())
    }

    pub(crate) fn mark_inbox_processed(
        &self,
        idempotency_key: &str,
        runtime_turn_id: Option<String>,
    ) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "processed", runtime_turn_id, None)
    }

    pub(crate) fn mark_inbox_admitted(
        &self,
        idempotency_key: &str,
        correlation: SurfaceTurnCorrelation,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        let (record, staged) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| -> Result<(), String> {
                record.stage_session_projections(projections)?;
                record.status = "processed".to_string();
                record.updated_at_ms = now_ms();
                record.runtime_session_id = Some(correlation.session_id.clone());
                record.runtime_turn_id = Some(correlation.turn_id.clone());
                record.correlation = Some(correlation);
                record.last_error = None;
                Ok(())
            },
        )?;
        staged?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: Some(record.message_id.clone()),
            kind: "inbox.processed".to_string(),
            status: "processed".to_string(),
            detail_json: serde_json::json!({
                "runtime_turn_id": record.runtime_turn_id,
                "execution_id": record.correlation.as_ref().map(|item| item.execution_id.clone()),
                "reply_idempotency_key": record.correlation.as_ref().map(|item| item.reply_idempotency_key.clone()),
            }),
            created_at_ms: now_ms(),
        })
    }

    pub(crate) fn record_inbox_terminal_delivery(
        &self,
        idempotency_key: &str,
        terminal_id: &str,
    ) -> Result<(), String> {
        let (record, updated) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| -> Result<(), String> {
                let correlation = record.correlation.as_mut().ok_or_else(|| {
                    format!("surface inbox `{idempotency_key}` has no turn correlation")
                })?;
                if correlation.terminal_id.as_deref() != Some(terminal_id) {
                    correlation.terminal_id = Some(terminal_id.to_string());
                    correlation.terminal_delivery_revision =
                        correlation.terminal_delivery_revision.saturating_add(1);
                }
                record.updated_at_ms = now_ms();
                Ok(())
            },
        )?;
        updated?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: Some(record.message_id.clone()),
            kind: "inbox.terminal_delivery_observed".to_string(),
            status: record.status.clone(),
            detail_json: serde_json::json!({
                "terminal_id": terminal_id,
                "terminal_delivery_revision": record.correlation.as_ref().map(|item| item.terminal_delivery_revision),
            }),
            created_at_ms: now_ms(),
        })
    }

    pub(crate) fn mark_inbox_replied(
        &self,
        idempotency_key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        let (record, staged) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| -> Result<(), String> {
                record.stage_session_projections(projections)?;
                record.status = "replied".to_string();
                record.updated_at_ms = now_ms();
                record.last_error = None;
                Ok(())
            },
        )?;
        staged?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface,
            delivery_id: None,
            message_id: Some(record.message_id),
            kind: "inbox.replied".to_string(),
            status: "replied".to_string(),
            detail_json: serde_json::json!({
                "session_projection_count": record.session_projections.len(),
            }),
            created_at_ms: now_ms(),
        })
    }

    pub(crate) fn mark_inbox_projection_applied(
        &self,
        idempotency_key: &str,
        event_id: &str,
        projected_at_ms: i64,
    ) -> Result<(), String> {
        let (_, result) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| {
                record.mark_session_projection_applied(event_id, projected_at_ms)?;
                record.updated_at_ms = now_ms();
                Ok::<(), String>(())
            },
        )?;
        result
    }

    pub(crate) fn stage_inbox_projections(
        &self,
        idempotency_key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        let (_, result) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| {
                record.stage_session_projections(projections)?;
                record.updated_at_ms = now_ms();
                Ok::<(), String>(())
            },
        )?;
        result
    }

    pub(crate) fn mark_inbox_projection_failed(
        &self,
        idempotency_key: &str,
        event_id: &str,
        error: &str,
    ) -> Result<(), String> {
        let (_, result) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| {
                record.mark_session_projection_failed(event_id, error)?;
                record.updated_at_ms = now_ms();
                Ok::<(), String>(())
            },
        )?;
        result
    }

    pub(crate) fn mark_inbox_reply_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "reply_failed", None, Some(error.into()))
    }

    pub(crate) fn mark_inbox_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "failed", None, Some(error.into()))
    }

    pub(crate) fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &ManagedAgentTriggerEvent,
        payload: &serde_json::Value,
    ) -> Result<SurfaceTriggerEventReceipt, String> {
        let surface = normalize_surface_id(surface);
        let idempotency_key = trigger.idempotency_key.clone();
        let mut state = self.lock_state()?;
        if let Some(existing) = state.trigger_events.get(&idempotency_key) {
            return Ok(SurfaceTriggerEventReceipt {
                record: existing.clone(),
                duplicate: true,
            });
        }
        let now = now_ms();
        let record = SurfaceTriggerEventRecord {
            idempotency_key: idempotency_key.clone(),
            surface: surface.clone(),
            event_type: event_type.to_string(),
            trigger: trigger.clone(),
            payload_json: payload.clone(),
            status: "received".to_string(),
            attempts: 0,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            next_retry_at_ms: Some(now),
            created_at_ms: now,
            updated_at_ms: now,
            accepted_at_ms: None,
            last_error: None,
        };
        state
            .trigger_events
            .insert(idempotency_key.clone(), record.clone());
        if !self.insert_record_if_absent(TRIGGER_EVENT_FILE, &record)? {
            let existing = self
                .lock_state()?
                .trigger_events
                .get(&idempotency_key)
                .cloned()
                .ok_or_else(|| {
                    "surface trigger idempotency race lost without durable row".to_string()
                })?;
            return Ok(SurfaceTriggerEventReceipt {
                record: existing,
                duplicate: true,
            });
        }
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface,
            delivery_id: None,
            message_id: None,
            kind: "trigger_event.received".to_string(),
            status: "received".to_string(),
            detail_json: serde_json::json!({
                "event_id": trigger.event_id,
                "event_type": event_type,
                "idempotency_key": record.idempotency_key,
            }),
            created_at_ms: now,
        })?;
        Ok(SurfaceTriggerEventReceipt {
            record,
            duplicate: false,
        })
    }

    pub(crate) fn mark_trigger_event_dispatching(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String> {
        let (record, claimed) = self.update_record_by_key(
            TRIGGER_EVENT_FILE,
            idempotency_key,
            |record: &mut SurfaceTriggerEventRecord| {
                if !matches!(record.status.as_str(), "received" | "retry_scheduled") {
                    return false;
                }
                record.status = "dispatching".to_string();
                record.attempts = record.attempts.saturating_add(1);
                record.next_retry_at_ms = None;
                record.last_error = None;
                record.updated_at_ms = now_ms();
                true
            },
        )?;
        if !claimed {
            return Ok(None);
        }
        self.push_trigger_event_delivery_event(&record)?;
        Ok(Some(record))
    }

    pub(crate) fn mark_trigger_event_accepted(
        &self,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.update_trigger_event(idempotency_key, |record| {
            record.status = "accepted".to_string();
            record.next_retry_at_ms = None;
            record.accepted_at_ms = Some(now_ms());
            record.last_error = None;
        })
    }

    pub(crate) fn mark_trigger_event_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let error = error.into();
        self.update_trigger_event(idempotency_key, |record| {
            record.last_error = Some(error.clone());
            if record.attempts < record.max_attempts {
                record.status = "retry_scheduled".to_string();
                record.next_retry_at_ms = Some(next_retry_at_ms(record.attempts));
            } else {
                record.status = "dead_letter".to_string();
                record.next_retry_at_ms = None;
            }
        })
    }

    pub(crate) fn retry_trigger_event(
        &self,
        surface: &str,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let surface = normalize_surface_id(surface);
        let current = self
            .lock_state()?
            .trigger_events
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| format!("surface trigger event `{idempotency_key}` not found"))?;
        if current.surface != surface {
            return Err(format!(
                "surface trigger event `{idempotency_key}` does not belong to surface `{surface}`"
            ));
        }
        if current.status != "dead_letter" {
            return Err(format!(
                "operator retry is only allowed for dead_letter trigger events; current status is {}",
                current.status
            ));
        }
        self.update_trigger_event(idempotency_key, |record| {
            record.status = "received".to_string();
            record.attempts = 0;
            record.next_retry_at_ms = Some(now_ms());
            record.last_error = None;
        })
    }

    pub(crate) fn queue_outbox(
        &self,
        request: &SurfaceSendRequest,
        source_session_id: Option<String>,
        reply_to_message_id: Option<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let surface = normalize_surface_id(&request.surface);
        let idempotency_key = request.idempotency_key.clone().unwrap_or_else(|| {
            outbound_idempotency_key(
                &surface,
                reply_to_message_id.as_deref(),
                &request.recipient,
                &request.text,
            )
        });
        let mut state = self.lock_state()?;
        if let Some(existing) = state.outbox.get(&idempotency_key) {
            return Ok(existing.clone());
        }
        let now = now_ms();
        let delivery_id = format!("surface-delivery-{}", uuid::Uuid::new_v4());
        let request_json = serde_json::to_value(request).map_err(|error| error.to_string())?;
        let record = SurfaceOutboxRecord {
            delivery_id: delivery_id.clone(),
            surface: surface.clone(),
            recipient: request.recipient.clone(),
            thread_id: request.thread.clone(),
            idempotency_key: idempotency_key.clone(),
            text_hash: hash_str(&request.text),
            text_summary: summarize_text(&request.text, 240),
            request_json,
            status: "queued".to_string(),
            attempts: 0,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            next_retry_at_ms: None,
            claim_owner: None,
            lease_until_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
            sent_at_ms: None,
            last_error: None,
            source_session_id,
            reply_to_message_id,
        };
        state.outbox.insert(idempotency_key.clone(), record.clone());
        if !self.insert_record_if_absent(OUTBOX_FILE, &record)? {
            return self
                .lock_state()?
                .outbox
                .get(&idempotency_key)
                .cloned()
                .ok_or_else(|| {
                    "surface outbox idempotency race lost without durable row".to_string()
                });
        }
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface,
            delivery_id: Some(delivery_id),
            message_id: record.reply_to_message_id.clone(),
            kind: "outbox.queued".to_string(),
            status: "queued".to_string(),
            detail_json: serde_json::json!({
                "recipient": record.recipient,
                "thread_id": record.thread_id,
                "text_summary": record.text_summary,
            }),
            created_at_ms: now,
        })?;
        Ok(record)
    }

    pub(crate) fn mark_delivery_sending(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOutboxRecord, String> {
        let state = self.lock_state()?;
        let key = state
            .outbox
            .iter()
            .find_map(|(key, record)| (record.delivery_id == delivery_id).then(|| key.clone()))
            .ok_or_else(|| format!("surface delivery `{delivery_id}` not found"))?;
        let (updated, claimed) =
            self.update_record_by_key(OUTBOX_FILE, &key, |record: &mut SurfaceOutboxRecord| {
                if is_terminal_outbox_status(&record.status) {
                    return 0_u8;
                }
                if !matches!(record.status.as_str(), "queued" | "retry_scheduled") {
                    return 2_u8;
                }
                record.status = "sending".to_string();
                record.attempts = record.attempts.saturating_add(1);
                record.updated_at_ms = now_ms();
                record.next_retry_at_ms = None;
                record.claim_owner = Some(format!("surface-delivery:{delivery_id}"));
                record.lease_until_ms = Some(now_ms().saturating_add(30_000));
                record.last_error = None;
                1_u8
            })?;
        if claimed == 2 {
            return Err(format!(
                "surface delivery `{delivery_id}` is already claimed or terminal ({})",
                updated.status
            ));
        }
        if claimed == 0 {
            return Ok(updated);
        }
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let status = if outbox_is_failure_notice(&updated) {
                "failure_notifying"
            } else {
                "replying"
            };
            let error = outbox_failure_reason(&updated);
            let _ = self.mark_inbox_status_by_message_id(&updated.surface, reply_to, status, error);
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_sent(
        &self,
        delivery_id: &str,
        result: &SurfaceOperationResult,
    ) -> Result<SurfaceOutboxRecord, String> {
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.status = "sent".to_string();
            record.updated_at_ms = now_ms();
            record.sent_at_ms = Some(record.updated_at_ms);
            record.next_retry_at_ms = None;
            record.claim_owner = None;
            record.lease_until_ms = None;
            record.last_error = None;
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: result.message_id.clone(),
            kind: "outbox.sent".to_string(),
            status: "sent".to_string(),
            detail_json: serde_json::to_value(result).unwrap_or_else(|_| serde_json::json!({})),
            created_at_ms: now_ms(),
        })?;
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let (status, error) = if outbox_is_failure_notice(&updated) {
                ("failed_notified", outbox_failure_reason(&updated))
            } else {
                ("replied", None)
            };
            let _ = self.mark_inbox_status_by_message_id(&updated.surface, reply_to, status, error);
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_failed(
        &self,
        delivery_id: &str,
        error: impl Into<String>,
        retryable: bool,
    ) -> Result<SurfaceOutboxRecord, String> {
        let error = error.into();
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.updated_at_ms = now_ms();
            record.last_error = Some(error.clone());
            record.claim_owner = None;
            record.lease_until_ms = None;
            if retryable && record.attempts < record.max_attempts {
                record.status = "retry_scheduled".to_string();
                record.next_retry_at_ms = Some(next_retry_at_ms(record.attempts));
            } else {
                record.status = "dead_letter".to_string();
                record.next_retry_at_ms = None;
            }
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: updated.reply_to_message_id.clone(),
            kind: if updated.status == "dead_letter" {
                "outbox.dead_letter".to_string()
            } else {
                "outbox.retry_scheduled".to_string()
            },
            status: updated.status.clone(),
            detail_json: serde_json::json!({
                "attempts": updated.attempts,
                "max_attempts": updated.max_attempts,
                "next_retry_at_ms": updated.next_retry_at_ms,
                "last_error": updated.last_error,
            }),
            created_at_ms: now_ms(),
        })?;
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let inbox_status = if updated.status == "dead_letter" {
                "reply_failed"
            } else {
                "reply_retry_scheduled"
            };
            let _ = self.mark_inbox_status_by_message_id(
                &updated.surface,
                reply_to,
                inbox_status,
                updated.last_error.clone(),
            );
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_dead_letter(
        &self,
        delivery_id: &str,
        reason: impl Into<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let reason = reason.into();
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.status = "dead_letter".to_string();
            record.updated_at_ms = now_ms();
            record.next_retry_at_ms = None;
            record.claim_owner = None;
            record.lease_until_ms = None;
            record.last_error = Some(reason.clone());
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: updated.reply_to_message_id.clone(),
            kind: "outbox.dead_letter".to_string(),
            status: "dead_letter".to_string(),
            detail_json: serde_json::json!({
                "reason": reason,
                "attempts": updated.attempts,
            }),
            created_at_ms: now_ms(),
        })?;
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let inbox_status = if updated.status == "dead_letter" {
                "reply_failed"
            } else {
                "reply_retry_scheduled"
            };
            let _ = self.mark_inbox_status_by_message_id(
                &updated.surface,
                reply_to,
                inbox_status,
                updated.last_error.clone(),
            );
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_replayed(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOutboxRecord, String> {
        let current = self
            .try_get_outbox_by_delivery(delivery_id)?
            .ok_or_else(|| format!("surface delivery `{delivery_id}` not found"))?;
        if current.status != "dead_letter" {
            return Err(format!(
                "operator retry is only allowed for dead_letter deliveries; current status is {}",
                current.status
            ));
        }
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.status = "queued".to_string();
            record.attempts = 0;
            record.updated_at_ms = now_ms();
            record.next_retry_at_ms = None;
            record.claim_owner = None;
            record.lease_until_ms = None;
            record.last_error = None;
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: updated.reply_to_message_id.clone(),
            kind: "outbox.operator_retry_requested".to_string(),
            status: "queued".to_string(),
            detail_json: serde_json::json!({"attempts": updated.attempts, "operator_action": "retry"}),
            created_at_ms: now_ms(),
        })?;
        Ok(updated)
    }

    pub(crate) fn archive_dead_letters(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        let now = now_ms();
        let mut state = self.lock_state()?;
        let mut archived = Vec::new();
        let keys = state
            .outbox
            .iter()
            .filter(|(_, record)| {
                record.surface == surface
                    && record.status == "dead_letter"
                    && older_than_ms.is_none_or(|threshold| record.updated_at_ms <= threshold)
            })
            .take(limit.max(1))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(record) = state.outbox.get_mut(&key) {
                record.status = "archived".to_string();
                record.updated_at_ms = now;
                record.next_retry_at_ms = None;
                archived.push(record.clone());
            }
        }
        for record in &archived {
            self.upsert_record(OUTBOX_FILE, record)?;
        }
        drop(state);
        for record in &archived {
            self.push_event(SurfaceDeliveryEvent {
                event_id: new_event_id(),
                surface: record.surface.clone(),
                delivery_id: Some(record.delivery_id.clone()),
                message_id: record.reply_to_message_id.clone(),
                kind: "outbox.dead_letter_archived".to_string(),
                status: "archived".to_string(),
                detail_json: serde_json::json!({
                    "attempts": record.attempts,
                    "max_attempts": record.max_attempts,
                    "last_error": record.last_error,
                }),
                created_at_ms: now_ms(),
            })?;
        }
        Ok(archived)
    }

    pub(crate) fn purge_archived_events(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<usize, String> {
        let surface = normalize_surface_id(surface);
        let limit = limit.max(1);
        let mut state = self.lock_state()?;
        let archived_delivery_ids = state
            .outbox
            .values()
            .filter(|record| {
                record.surface == surface
                    && record.status == "archived"
                    && older_than_ms.is_none_or(|threshold| record.updated_at_ms <= threshold)
            })
            .map(|record| record.delivery_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if archived_delivery_ids.is_empty() {
            return Ok(0);
        }

        let purged_event_ids = state
            .events
            .iter()
            .filter(|(_, event)| {
                event.surface == surface
                    && event
                        .delivery_id
                        .as_ref()
                        .is_some_and(|delivery_id| archived_delivery_ids.contains(delivery_id))
                    && older_than_ms.is_none_or(|threshold| event.created_at_ms <= threshold)
            })
            .take(limit)
            .map(|(event_id, _)| event_id.clone())
            .collect::<Vec<_>>();

        for event_id in &purged_event_ids {
            state.events.remove(event_id);
        }
        let remaining = state.events.values().cloned().collect::<Vec<_>>();
        drop(state);
        if !purged_event_ids.is_empty() {
            self.replace_records_transaction(EVENT_FILE, &remaining)?;
        }
        Ok(purged_event_ids.len())
    }

    fn try_get_outbox_by_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<SurfaceOutboxRecord>, String> {
        Ok(self
            .lock_state()?
            .outbox
            .values()
            .find(|record| record.delivery_id == delivery_id)
            .cloned())
    }

    fn try_due_retry_deliveries(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.recover_expired_outbox_claims()?;
        let now = now_ms();
        Ok(self
            .lock_state()?
            .outbox
            .values()
            .filter(|record| {
                record.status == "retry_scheduled"
                    && record.attempts < record.max_attempts
                    && record.next_retry_at_ms.is_some_and(|due| due <= now)
            })
            .cloned()
            .collect())
    }

    fn recover_expired_outbox_claims(&self) -> Result<(), String> {
        let now = now_ms();
        let expired = self
            .lock_state()?
            .outbox
            .values()
            .filter(|record| {
                record.status == "sending"
                    && record.lease_until_ms.is_some_and(|lease| lease <= now)
            })
            .map(|record| record.delivery_id.clone())
            .collect::<Vec<_>>();
        for delivery_id in expired {
            self.update_outbox_by_delivery(&delivery_id, |record| {
                record.status = "retry_scheduled".to_string();
                record.next_retry_at_ms = Some(now);
                record.claim_owner = None;
                record.lease_until_ms = None;
                record.updated_at_ms = now;
                record.last_error = Some("surface outbound delivery claim expired".to_string());
            })?;
        }
        Ok(())
    }

    fn try_due_trigger_event_retries(&self) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        let now = now_ms();
        Ok(self
            .lock_state()?
            .trigger_events
            .values()
            .filter(|record| {
                matches!(record.status.as_str(), "received" | "retry_scheduled")
                    && record.attempts < record.max_attempts
                    && record.next_retry_at_ms.is_some_and(|due| due <= now)
            })
            .cloned()
            .collect())
    }

    fn try_get_inbox_message(
        &self,
        surface: &str,
        message_id: &str,
    ) -> Result<Option<SurfaceInboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        Ok(self
            .lock_state()?
            .inbox
            .values()
            .find(|record| {
                record.surface == surface
                    && (record.message_id == message_id || record.id == message_id)
            })
            .cloned())
    }

    fn try_list_inbox(&self, surface: &str) -> Result<Vec<SurfaceInboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        Ok(self
            .lock_state()?
            .inbox
            .values()
            .filter(|record| record.surface == surface)
            .cloned()
            .collect())
    }

    fn try_list_outbox(&self, surface: &str) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        Ok(self
            .lock_state()?
            .outbox
            .values()
            .filter(|record| record.surface == surface)
            .cloned()
            .collect())
    }

    fn try_list_all_inbox(&self) -> Result<Vec<SurfaceInboxRecord>, String> {
        Ok(self.lock_state()?.inbox.values().cloned().collect())
    }

    fn try_list_all_outbox(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        Ok(self.lock_state()?.outbox.values().cloned().collect())
    }

    fn try_list_trigger_events(
        &self,
        surface: &str,
    ) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        let surface = normalize_surface_id(surface);
        Ok(self
            .lock_state()?
            .trigger_events
            .values()
            .filter(|record| record.surface == surface)
            .cloned()
            .collect())
    }

    fn try_list_delivery_events(&self, surface: &str) -> Result<Vec<SurfaceDeliveryEvent>, String> {
        let surface = normalize_surface_id(surface);
        Ok(self
            .lock_state()?
            .events
            .values()
            .filter(|event| event.surface == surface)
            .cloned()
            .collect())
    }

    fn try_snapshot(&self, surface: &str) -> Result<SurfaceMessageSnapshot, String> {
        let surface = normalize_surface_id(surface);
        let inbox = self.try_list_inbox(&surface)?;
        let outbox = self.try_list_outbox(&surface)?;
        let trigger_events = self.try_list_trigger_events(&surface)?;
        let active_inbox = inbox
            .iter()
            .filter(|record| is_active_inbox_status(&record.status))
            .cloned()
            .collect();
        let terminal_inbox = inbox
            .iter()
            .filter(|record| !is_active_inbox_status(&record.status))
            .cloned()
            .collect();
        let active_trigger_events = trigger_events
            .iter()
            .filter(|record| is_active_trigger_event_status(&record.status))
            .cloned()
            .collect();
        let failed_trigger_events = trigger_events
            .iter()
            .filter(|record| record.status == "dead_letter")
            .cloned()
            .collect();
        let active_outbox = outbox
            .iter()
            .filter(|record| is_active_outbox_status(&record.status))
            .cloned()
            .collect();
        let terminal_outbox = outbox
            .iter()
            .filter(|record| is_terminal_outbox_status(&record.status))
            .cloned()
            .collect();
        let dead_letters = outbox
            .iter()
            .filter(|record| record.status == "dead_letter")
            .cloned()
            .collect();
        let archived_outbox = outbox
            .iter()
            .filter(|record| record.status == "archived")
            .cloned()
            .collect::<Vec<_>>();
        let archived_count = archived_outbox.len();
        Ok(SurfaceMessageSnapshot {
            kind: "surface.message_snapshot",
            surface: surface.clone(),
            message_root: self.root.clone(),
            inbox,
            active_inbox,
            terminal_inbox,
            trigger_events,
            active_trigger_events,
            failed_trigger_events,
            outbox,
            active_outbox,
            terminal_outbox,
            deliveries: self.try_list_delivery_events(&surface)?,
            dead_letters,
            archived_outbox,
            archived_count,
        })
    }

    fn update_inbox_status(
        &self,
        idempotency_key: &str,
        status: &str,
        runtime_turn_id: Option<String>,
        error: Option<String>,
    ) -> Result<(), String> {
        let status_owned = status.to_string();
        let (record, ()) = self.update_record_by_key(
            INBOX_FILE,
            idempotency_key,
            |record: &mut SurfaceInboxRecord| {
                record.status = status_owned;
                record.updated_at_ms = now_ms();
                if runtime_turn_id.is_some() {
                    record.runtime_turn_id = runtime_turn_id;
                }
                record.last_error = error;
            },
        )?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface,
            delivery_id: None,
            message_id: Some(record.message_id),
            kind: format!("inbox.{status}"),
            status: status.to_string(),
            detail_json: serde_json::json!({
                "runtime_turn_id": record.runtime_turn_id,
                "last_error": record.last_error,
            }),
            created_at_ms: now_ms(),
        })
    }

    fn update_trigger_event(
        &self,
        idempotency_key: &str,
        update: impl FnOnce(&mut SurfaceTriggerEventRecord),
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let (record, ()) = self.update_record_by_key(
            TRIGGER_EVENT_FILE,
            idempotency_key,
            |record: &mut SurfaceTriggerEventRecord| {
                update(record);
                record.updated_at_ms = now_ms();
            },
        )?;
        self.push_trigger_event_delivery_event(&record)?;
        Ok(record)
    }

    fn push_trigger_event_delivery_event(
        &self,
        record: &SurfaceTriggerEventRecord,
    ) -> Result<(), String> {
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: None,
            kind: format!("trigger_event.{}", record.status),
            status: record.status.clone(),
            detail_json: serde_json::json!({
                "event_id": record.trigger.event_id,
                "event_type": record.event_type,
                "idempotency_key": record.idempotency_key,
                "attempts": record.attempts,
                "max_attempts": record.max_attempts,
                "next_retry_at_ms": record.next_retry_at_ms,
                "last_error": record.last_error,
            }),
            created_at_ms: now_ms(),
        })?;
        Ok(())
    }

    fn mark_inbox_status_by_message_id(
        &self,
        surface: &str,
        message_id: &str,
        status: &str,
        error: Option<String>,
    ) -> Result<Option<SurfaceInboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        let state = self.lock_state()?;
        let Some(key) = state.inbox.iter().find_map(|(key, record)| {
            (record.surface == surface && record.message_id == message_id).then(|| key.clone())
        }) else {
            return Ok(None);
        };
        let status_owned = status.to_string();
        let (record, changed) =
            self.update_record_by_key(INBOX_FILE, &key, |record: &mut SurfaceInboxRecord| {
                if record.status == status_owned && record.last_error == error {
                    return false;
                }
                record.status = status_owned;
                record.updated_at_ms = now_ms();
                record.last_error = error;
                true
            })?;
        if !changed {
            return Ok(Some(record));
        }
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: Some(record.message_id.clone()),
            kind: format!("inbox.{status}"),
            status: status.to_string(),
            detail_json: serde_json::json!({
                "runtime_turn_id": record.runtime_turn_id,
                "last_error": record.last_error,
            }),
            created_at_ms: now_ms(),
        })?;
        Ok(Some(record))
    }

    fn update_outbox_by_delivery(
        &self,
        delivery_id: &str,
        update: impl FnOnce(&mut SurfaceOutboxRecord),
    ) -> Result<SurfaceOutboxRecord, String> {
        let state = self.lock_state()?;
        let key = state
            .outbox
            .iter()
            .find_map(|(key, record)| (record.delivery_id == delivery_id).then(|| key.clone()))
            .ok_or_else(|| format!("surface delivery `{delivery_id}` not found"))?;
        self.update_record_by_key(OUTBOX_FILE, &key, |record: &mut SurfaceOutboxRecord| {
            update(record)
        })
        .map(|(record, ())| record)
    }

    fn push_event(&self, event: SurfaceDeliveryEvent) -> Result<(), String> {
        self.upsert_record(EVENT_FILE, &event)
    }

    fn update_record_by_key<T, R>(
        &self,
        file: &str,
        record_key: &str,
        update: impl FnOnce(&mut T) -> R,
    ) -> Result<(T, R), String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let table = table_for_file(file)?;
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let payload = transaction
            .query_row(
                &format!("SELECT payload_json FROM {table} WHERE record_key=?1"),
                params![record_key],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| error.to_string())?;
        let mut record = serde_json::from_str::<T>(&payload).map_err(|error| error.to_string())?;
        let result = update(&mut record);
        let value = serde_json::to_value(&record).map_err(|error| error.to_string())?;
        upsert_json_record(&transaction, file, &value)?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok((record, result))
    }

    fn upsert_record<T: Serialize>(&self, file: &str, record: &T) -> Result<(), String> {
        let value = serde_json::to_value(record).map_err(|error| error.to_string())?;
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        upsert_json_record(&transaction, file, &value)?;
        transaction.commit().map_err(|error| error.to_string())
    }

    fn insert_record_if_absent<T: Serialize>(
        &self,
        file: &str,
        record: &T,
    ) -> Result<bool, String> {
        let value = serde_json::to_value(record).map_err(|error| error.to_string())?;
        let table = table_for_file(file)?;
        let (key, surface, status, next_retry_at_ms, updated_at_ms) = record_columns(file, &value)?;
        let payload = value.to_string();
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        connection
            .execute(
                &format!(
                    "INSERT OR IGNORE INTO {table}(
                       record_key, surface, status, next_retry_at_ms, updated_at_ms, payload_json
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)"
                ),
                params![
                    key,
                    surface,
                    status,
                    next_retry_at_ms,
                    updated_at_ms,
                    payload
                ],
            )
            .map(|changed| changed == 1)
            .map_err(|error| error.to_string())
    }

    fn replace_records_transaction<T: Serialize>(
        &self,
        file: &str,
        records: &[T],
    ) -> Result<(), String> {
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        transaction
            .execute(&format!("DELETE FROM {}", table_for_file(file)?), [])
            .map_err(|error| error.to_string())?;
        for record in records {
            let value = serde_json::to_value(record).map_err(|error| error.to_string())?;
            upsert_json_record(&transaction, file, &value)?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }

    fn lock_state(&self) -> Result<SurfaceMessageState, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_database_state(&connection)
    }

    fn import_legacy_jsonl_once(&self) -> Result<(), String> {
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let imported = connection
            .query_row(
                "SELECT value FROM surface_store_meta WHERE key = 'legacy_jsonl_import_v1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if imported.is_some() {
            return Ok(());
        }
        let state = load_legacy_state(&self.root)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        persist_state_transaction(&transaction, &state)?;
        let evidence = legacy_import_evidence(&self.root, &state);
        transaction
            .execute(
                "INSERT INTO surface_store_meta(key, value) VALUES('legacy_jsonl_import_v1', ?1)",
                params![evidence.to_string()],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    fn reconcile_after_restart(&self) -> Result<(), String> {
        {
            let connection = self
                .executor
                .checkout()
                .map_err(|error| error.to_string())?;
            let now = now_ms();
            connection
                .execute(
                    "UPDATE surface_ingress_frame
                     SET status='retry_scheduled', claim_owner=NULL, lease_until_ms=NULL,
                         next_retry_at_ms=?1, updated_at_ms=?1,
                         last_error='gateway restarted before ingress claim completed'
                     WHERE status='claimed'",
                    params![now],
                )
                .map_err(|error| error.to_string())?;
        }
        let mut state = self.lock_state()?;
        normalize_terminal_outbox_state(&mut state);
        normalize_trigger_event_state(&mut state);
        reconcile_inbox_with_outbox(&mut state);
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        persist_state_transaction(&transaction, &state)?;
        transaction.commit().map_err(|error| error.to_string())
    }
}

/// Gateway's current SQLite implementation of the storage-neutral Surface
/// ledger.  The host only retains this object through `SurfaceMessageLedger`;
/// a future PostgreSQL adapter therefore changes composition, not callers.
impl SurfaceMessageLedger for SqliteSurfaceMessageStore {
    fn diagnostic_root(&self) -> PathBuf {
        self.root.clone()
    }
    fn persist_ingress_frame(&self, frame: &SurfaceFrame) -> Result<String, String> {
        self.persist_ingress_frame(frame)
    }
    fn claim_ingress_frames(
        &self,
        owner: &str,
        limit: usize,
        lease_ms: i64,
    ) -> Result<Vec<SurfaceIngressClaim>, String> {
        self.claim_ingress_frames(owner, limit, lease_ms)
    }
    fn complete_ingress_frame(&self, key: &str) -> Result<(), String> {
        self.complete_ingress_frame(key)
    }
    fn fail_ingress_frame(&self, key: &str, error: &str) -> Result<(), String> {
        self.fail_ingress_frame(key, error)
    }
    fn record_inbox_received(
        &self,
        surface: &str,
        message_id: &str,
        payload: &serde_json::Value,
        session: &str,
        thread: Option<String>,
        sender: Option<String>,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<SurfaceInboxReceipt, String> {
        self.record_inbox_received(
            surface,
            message_id,
            payload,
            session,
            thread,
            sender,
            projections,
        )
    }
    fn mark_inbox_processing(
        &self,
        key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.mark_inbox_processing(key, projections)
    }
    fn mark_inbox_processed(&self, key: &str, turn: Option<String>) -> Result<(), String> {
        self.mark_inbox_processed(key, turn)
    }
    fn mark_inbox_admitted(
        &self,
        key: &str,
        correlation: SurfaceTurnCorrelation,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.mark_inbox_admitted(key, correlation, projections)
    }
    fn record_inbox_terminal_delivery(&self, key: &str, terminal: &str) -> Result<(), String> {
        self.record_inbox_terminal_delivery(key, terminal)
    }
    fn mark_inbox_replied(
        &self,
        key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.mark_inbox_replied(key, projections)
    }
    fn stage_inbox_projections(
        &self,
        key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.stage_inbox_projections(key, projections)
    }
    fn mark_inbox_projection_applied(
        &self,
        key: &str,
        event_id: &str,
        projected_at_ms: i64,
    ) -> Result<(), String> {
        self.mark_inbox_projection_applied(key, event_id, projected_at_ms)
    }
    fn mark_inbox_projection_failed(
        &self,
        key: &str,
        event_id: &str,
        error: &str,
    ) -> Result<(), String> {
        self.mark_inbox_projection_failed(key, event_id, error)
    }
    fn mark_inbox_reply_failed(&self, key: &str, error: &str) -> Result<(), String> {
        self.mark_inbox_reply_failed(key, error)
    }
    fn mark_inbox_failed(&self, key: &str, error: &str) -> Result<(), String> {
        self.mark_inbox_failed(key, error)
    }
    fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &ManagedAgentTriggerEvent,
        payload: &serde_json::Value,
    ) -> Result<SurfaceTriggerEventReceipt, String> {
        self.record_trigger_event_received(surface, event_type, trigger, payload)
    }
    fn mark_trigger_event_dispatching(
        &self,
        key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String> {
        self.mark_trigger_event_dispatching(key)
    }
    fn mark_trigger_event_accepted(&self, key: &str) -> Result<SurfaceTriggerEventRecord, String> {
        self.mark_trigger_event_accepted(key)
    }
    fn mark_trigger_event_failed(
        &self,
        key: &str,
        error: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.mark_trigger_event_failed(key, error)
    }
    fn retry_trigger_event(
        &self,
        surface: &str,
        key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.retry_trigger_event(surface, key)
    }
    fn queue_outbox(
        &self,
        request: &SurfaceSendRequest,
        session: Option<String>,
        reply_to: Option<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.queue_outbox(request, session, reply_to)
    }
    fn mark_delivery_sending(&self, id: &str) -> Result<SurfaceOutboxRecord, String> {
        self.mark_delivery_sending(id)
    }
    fn mark_delivery_sent(
        &self,
        id: &str,
        result: &SurfaceOperationResult,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.mark_delivery_sent(id, result)
    }
    fn mark_delivery_failed(
        &self,
        id: &str,
        error: &str,
        retryable: bool,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.mark_delivery_failed(id, error, retryable)
    }
    fn mark_delivery_dead_letter(
        &self,
        id: &str,
        reason: &str,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.mark_delivery_dead_letter(id, reason)
    }
    fn mark_delivery_replayed(&self, id: &str) -> Result<SurfaceOutboxRecord, String> {
        self.mark_delivery_replayed(id)
    }
    fn archive_dead_letters(
        &self,
        surface: &str,
        older_than: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.archive_dead_letters(surface, older_than, limit)
    }
    fn purge_archived_events(
        &self,
        surface: &str,
        older_than: Option<i64>,
        limit: usize,
    ) -> Result<usize, String> {
        self.purge_archived_events(surface, older_than, limit)
    }
    fn get_outbox_by_delivery(&self, id: &str) -> Result<Option<SurfaceOutboxRecord>, String> {
        self.try_get_outbox_by_delivery(id)
    }
    fn due_retry_deliveries(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.try_due_retry_deliveries()
    }
    fn due_trigger_event_retries(&self) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        self.try_due_trigger_event_retries()
    }
    fn get_inbox_message(
        &self,
        surface: &str,
        id: &str,
    ) -> Result<Option<SurfaceInboxRecord>, String> {
        self.try_get_inbox_message(surface, id)
    }
    fn list_inbox(&self, surface: &str) -> Result<Vec<SurfaceInboxRecord>, String> {
        self.try_list_inbox(surface)
    }
    fn list_outbox(&self, surface: &str) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.try_list_outbox(surface)
    }
    fn list_all_inbox(&self) -> Result<Vec<SurfaceInboxRecord>, String> {
        self.try_list_all_inbox()
    }
    fn list_all_outbox(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.try_list_all_outbox()
    }
    fn list_trigger_events(&self, surface: &str) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        self.try_list_trigger_events(surface)
    }
    fn list_delivery_events(&self, surface: &str) -> Result<Vec<SurfaceDeliveryEvent>, String> {
        self.try_list_delivery_events(surface)
    }
    fn snapshot(&self, surface: &str) -> Result<SurfaceMessageSnapshot, String> {
        self.try_snapshot(surface)
    }
    fn export_migration_snapshot(&self) -> Result<SurfaceMessageLedgerMigrationSnapshot, String> {
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let state = load_database_state(&transaction)?;
        let ingress_frames = load_ingress_frames(&transaction)?;
        transaction.commit().map_err(|error| error.to_string())?;
        let snapshot = SurfaceMessageLedgerMigrationSnapshot {
            inbox: state.inbox.into_values().collect(),
            outbox: state.outbox.into_values().collect(),
            trigger_events: state.trigger_events.into_values().collect(),
            delivery_events: state.events.into_values().collect(),
            ingress_frames,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
    fn import_migration_snapshot(
        &self,
        snapshot: &SurfaceMessageLedgerMigrationSnapshot,
    ) -> Result<(), String> {
        snapshot.validate()?;
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        for table in [
            "surface_inbox",
            "surface_outbox",
            "surface_trigger_event",
            "surface_delivery_event",
            "surface_ingress_frame",
        ] {
            let count = transaction
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(|error| error.to_string())?;
            if count != 0 {
                return Err(format!(
                    "surface migration target table `{table}` is not empty"
                ));
            }
        }
        let state = SurfaceMessageState {
            inbox: snapshot
                .inbox
                .iter()
                .cloned()
                .map(|record| (record.idempotency_key.clone(), record))
                .collect(),
            outbox: snapshot
                .outbox
                .iter()
                .cloned()
                .map(|record| (record.idempotency_key.clone(), record))
                .collect(),
            trigger_events: snapshot
                .trigger_events
                .iter()
                .cloned()
                .map(|record| (record.idempotency_key.clone(), record))
                .collect(),
            events: snapshot
                .delivery_events
                .iter()
                .cloned()
                .map(|record| (record.event_id.clone(), record))
                .collect(),
        };
        persist_state_transaction(&transaction, &state)?;
        for record in &snapshot.ingress_frames {
            insert_ingress_frame(&transaction, record)?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }
}

#[cfg(test)]
impl SqliteSurfaceMessageStore {
    // Test-only projections keep existing behavior assertions concise.  They
    // are not available to Gateway production callers, which use the fallible
    // `SurfaceMessageLedger` contract above.
    fn get_outbox_by_delivery(&self, id: &str) -> Option<SurfaceOutboxRecord> {
        self.try_get_outbox_by_delivery(id)
            .expect("test surface ledger read")
    }
    fn due_retry_deliveries(&self) -> Vec<SurfaceOutboxRecord> {
        self.try_due_retry_deliveries()
            .expect("test surface ledger read")
    }
    fn due_trigger_event_retries(&self) -> Vec<SurfaceTriggerEventRecord> {
        self.try_due_trigger_event_retries()
            .expect("test surface ledger read")
    }
    fn get_inbox_message(&self, surface: &str, id: &str) -> Option<SurfaceInboxRecord> {
        self.try_get_inbox_message(surface, id)
            .expect("test surface ledger read")
    }
    fn list_inbox(&self, surface: &str) -> Vec<SurfaceInboxRecord> {
        self.try_list_inbox(surface)
            .expect("test surface ledger read")
    }
    fn list_outbox(&self, surface: &str) -> Vec<SurfaceOutboxRecord> {
        self.try_list_outbox(surface)
            .expect("test surface ledger read")
    }
    fn list_all_inbox(&self) -> Vec<SurfaceInboxRecord> {
        self.try_list_all_inbox().expect("test surface ledger read")
    }
    fn list_all_outbox(&self) -> Vec<SurfaceOutboxRecord> {
        self.try_list_all_outbox()
            .expect("test surface ledger read")
    }
    fn list_trigger_events(&self, surface: &str) -> Vec<SurfaceTriggerEventRecord> {
        self.try_list_trigger_events(surface)
            .expect("test surface ledger read")
    }
    fn list_delivery_events(&self, surface: &str) -> Vec<SurfaceDeliveryEvent> {
        self.try_list_delivery_events(surface)
            .expect("test surface ledger read")
    }
    fn snapshot(&self, surface: &str) -> SurfaceMessageSnapshot {
        self.try_snapshot(surface)
            .expect("test surface ledger read")
    }
}

fn initialize_database(connection: &Connection) -> Result<(), String> {
    connection
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|error| error.to_string())?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| error.to_string())?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|error| error.to_string())?;
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS surface_store_meta(
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS surface_inbox(
                 record_key TEXT PRIMARY KEY,
                 surface TEXT NOT NULL,
                 status TEXT NOT NULL,
                 next_retry_at_ms INTEGER,
                 updated_at_ms INTEGER NOT NULL,
                 payload_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS surface_outbox(
                 record_key TEXT PRIMARY KEY,
                 surface TEXT NOT NULL,
                 status TEXT NOT NULL,
                 next_retry_at_ms INTEGER,
                 updated_at_ms INTEGER NOT NULL,
                 payload_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS surface_trigger_event(
                 record_key TEXT PRIMARY KEY,
                 surface TEXT NOT NULL,
                 status TEXT NOT NULL,
                 next_retry_at_ms INTEGER,
                 updated_at_ms INTEGER NOT NULL,
                 payload_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS surface_delivery_event(
                 record_key TEXT PRIMARY KEY,
                 surface TEXT NOT NULL,
                 status TEXT NOT NULL,
                 next_retry_at_ms INTEGER,
                 updated_at_ms INTEGER NOT NULL,
                 payload_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS surface_ingress_frame(
                 record_key TEXT PRIMARY KEY,
                 surface TEXT NOT NULL,
                 session_id TEXT NOT NULL,
                 status TEXT NOT NULL,
                 attempts INTEGER NOT NULL DEFAULT 0,
                 max_attempts INTEGER NOT NULL DEFAULT 5,
                 next_retry_at_ms INTEGER,
                 claim_owner TEXT,
                 lease_until_ms INTEGER,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL,
                 payload_json TEXT NOT NULL,
                 last_error TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_surface_inbox_surface_status
                 ON surface_inbox(surface, status, updated_at_ms);
             CREATE INDEX IF NOT EXISTS idx_surface_outbox_due
                 ON surface_outbox(status, next_retry_at_ms, surface);
             CREATE INDEX IF NOT EXISTS idx_surface_trigger_due
                 ON surface_trigger_event(status, next_retry_at_ms, surface);
             CREATE INDEX IF NOT EXISTS idx_surface_delivery_surface_created
                 ON surface_delivery_event(surface, updated_at_ms);
             CREATE INDEX IF NOT EXISTS idx_surface_ingress_claim
                 ON surface_ingress_frame(status, next_retry_at_ms, lease_until_ms, session_id, created_at_ms);",
        )
        .map_err(|error| error.to_string())
}

fn table_for_file(file: &str) -> Result<&'static str, String> {
    match file {
        INBOX_FILE => Ok("surface_inbox"),
        OUTBOX_FILE => Ok("surface_outbox"),
        TRIGGER_EVENT_FILE => Ok("surface_trigger_event"),
        EVENT_FILE => Ok("surface_delivery_event"),
        other => Err(format!("unknown surface durable record family `{other}`")),
    }
}

fn record_columns(
    file: &str,
    value: &serde_json::Value,
) -> Result<(String, String, String, Option<i64>, i64), String> {
    let string = |field: &str| {
        value
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string)
            .ok_or_else(|| format!("surface durable record missing `{field}`"))
    };
    let key = match file {
        INBOX_FILE | OUTBOX_FILE | TRIGGER_EVENT_FILE => string("idempotency_key")?,
        EVENT_FILE => string("event_id")?,
        other => return Err(format!("unknown surface durable record family `{other}`")),
    };
    let surface = string("surface")?;
    let status = string("status")?;
    let next_retry_at_ms = value
        .get("next_retry_at_ms")
        .and_then(serde_json::Value::as_i64);
    let updated_at_ms = value
        .get("updated_at_ms")
        .or_else(|| value.get("created_at_ms"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(now_ms);
    Ok((key, surface, status, next_retry_at_ms, updated_at_ms))
}

fn upsert_json_record(
    transaction: &Transaction<'_>,
    file: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    let table = table_for_file(file)?;
    let (key, surface, status, next_retry_at_ms, updated_at_ms) = record_columns(file, value)?;
    let payload = serde_json::to_string(value).map_err(|error| error.to_string())?;
    transaction
        .execute(
            &format!(
                "INSERT INTO {table}(record_key, surface, status, next_retry_at_ms, updated_at_ms, payload_json)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(record_key) DO UPDATE SET
                   surface=excluded.surface,
                   status=excluded.status,
                   next_retry_at_ms=excluded.next_retry_at_ms,
                   updated_at_ms=excluded.updated_at_ms,
                   payload_json=excluded.payload_json"
            ),
            params![key, surface, status, next_retry_at_ms, updated_at_ms, payload],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn load_database_records<T>(
    connection: &Connection,
    table: &str,
) -> Result<BTreeMap<String, T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    let mut statement = connection
        .prepare(&format!("SELECT record_key, payload_json FROM {table}"))
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| error.to_string())?;
    let mut records = BTreeMap::new();
    for row in rows {
        let (key, payload) = row.map_err(|error| error.to_string())?;
        records.insert(
            key,
            serde_json::from_str::<T>(&payload).map_err(|error| error.to_string())?,
        );
    }
    Ok(records)
}

fn load_database_state(connection: &Connection) -> Result<SurfaceMessageState, String> {
    Ok(SurfaceMessageState {
        inbox: load_database_records(connection, "surface_inbox")?,
        outbox: load_database_records(connection, "surface_outbox")?,
        trigger_events: load_database_records(connection, "surface_trigger_event")?,
        events: load_database_records(connection, "surface_delivery_event")?,
    })
}

fn load_ingress_frames(connection: &Connection) -> Result<Vec<SurfaceIngressFrameRecord>, String> {
    let mut statement = connection
        .prepare(
            "SELECT record_key, surface, session_id, status, attempts, max_attempts,
                    next_retry_at_ms, claim_owner, lease_until_ms, created_at_ms, updated_at_ms,
                    payload_json, last_error
               FROM surface_ingress_frame
              ORDER BY record_key ASC",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            let payload_json = row.get::<_, String>(11)?;
            let frame = serde_json::from_str(&payload_json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    11,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(SurfaceIngressFrameRecord {
                record_key: row.get(0)?,
                surface: row.get(1)?,
                session_id: row.get(2)?,
                status: row.get(3)?,
                attempts: row.get(4)?,
                max_attempts: row.get(5)?,
                next_retry_at_ms: row.get(6)?,
                claim_owner: row.get(7)?,
                lease_until_ms: row.get(8)?,
                created_at_ms: row.get(9)?,
                updated_at_ms: row.get(10)?,
                frame,
                last_error: row.get(12)?,
            })
        })
        .map_err(|error| error.to_string())?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())
}

fn insert_ingress_frame(
    transaction: &Transaction<'_>,
    record: &SurfaceIngressFrameRecord,
) -> Result<(), String> {
    let payload_json = serde_json::to_string(&record.frame).map_err(|error| error.to_string())?;
    transaction
        .execute(
            "INSERT INTO surface_ingress_frame(
                record_key, surface, session_id, status, attempts, max_attempts,
                next_retry_at_ms, claim_owner, lease_until_ms, created_at_ms, updated_at_ms,
                payload_json, last_error
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                &record.record_key,
                &record.surface,
                &record.session_id,
                &record.status,
                record.attempts,
                record.max_attempts,
                record.next_retry_at_ms,
                &record.claim_owner,
                record.lease_until_ms,
                record.created_at_ms,
                record.updated_at_ms,
                payload_json,
                &record.last_error,
            ],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn persist_state_transaction(
    transaction: &Transaction<'_>,
    state: &SurfaceMessageState,
) -> Result<(), String> {
    for table in [
        "surface_inbox",
        "surface_outbox",
        "surface_trigger_event",
        "surface_delivery_event",
    ] {
        transaction
            .execute(&format!("DELETE FROM {table}"), [])
            .map_err(|error| error.to_string())?;
    }
    for record in state.inbox.values() {
        upsert_json_record(
            transaction,
            INBOX_FILE,
            &serde_json::to_value(record).map_err(|error| error.to_string())?,
        )?;
    }
    for record in state.outbox.values() {
        upsert_json_record(
            transaction,
            OUTBOX_FILE,
            &serde_json::to_value(record).map_err(|error| error.to_string())?,
        )?;
    }
    for record in state.trigger_events.values() {
        upsert_json_record(
            transaction,
            TRIGGER_EVENT_FILE,
            &serde_json::to_value(record).map_err(|error| error.to_string())?,
        )?;
    }
    for record in state.events.values() {
        upsert_json_record(
            transaction,
            EVENT_FILE,
            &serde_json::to_value(record).map_err(|error| error.to_string())?,
        )?;
    }
    Ok(())
}

fn legacy_import_evidence(root: &Path, state: &SurfaceMessageState) -> serde_json::Value {
    let mut hasher = Sha256::new();
    for file in [INBOX_FILE, OUTBOX_FILE, TRIGGER_EVENT_FILE, EVENT_FILE] {
        if let Ok(bytes) = fs::read(root.join(file)) {
            hasher.update(file.as_bytes());
            hasher.update(&bytes);
        }
    }
    serde_json::json!({
        "inbox": state.inbox.len(),
        "outbox": state.outbox.len(),
        "trigger_events": state.trigger_events.len(),
        "delivery_events": state.events.len(),
        "sha256": format!("{:x}", hasher.finalize()),
        "imported_at_ms": now_ms(),
    })
}

pub(crate) fn inbound_idempotency_key(surface: &str, message_id: &str) -> String {
    format!("{}:{}", normalize_surface_id(surface), message_id)
}

fn outbound_idempotency_key(
    surface: &str,
    reply_to_message_id: Option<&str>,
    recipient: &str,
    text: &str,
) -> String {
    format!(
        "{}:{}:{}:{}",
        normalize_surface_id(surface),
        reply_to_message_id.unwrap_or("manual"),
        recipient,
        hash_str(text)
    )
}

fn load_legacy_state(root: &Path) -> Result<SurfaceMessageState, String> {
    let mut state = SurfaceMessageState {
        inbox: read_latest(root.join(INBOX_FILE), |record: &SurfaceInboxRecord| {
            record.idempotency_key.clone()
        })?,
        outbox: read_latest(root.join(OUTBOX_FILE), |record: &SurfaceOutboxRecord| {
            record.idempotency_key.clone()
        })?,
        trigger_events: read_latest(
            root.join(TRIGGER_EVENT_FILE),
            |record: &SurfaceTriggerEventRecord| record.idempotency_key.clone(),
        )?,
        events: read_latest(root.join(EVENT_FILE), |record: &SurfaceDeliveryEvent| {
            record.event_id.clone()
        })?,
    };
    normalize_terminal_outbox_state(&mut state);
    normalize_trigger_event_state(&mut state);
    reconcile_inbox_with_outbox(&mut state);
    Ok(state)
}

fn normalize_trigger_event_state(state: &mut SurfaceMessageState) {
    let now = now_ms();
    for record in state.trigger_events.values_mut() {
        if matches!(record.status.as_str(), "received" | "dispatching") {
            record.status = "retry_scheduled".to_string();
            record.next_retry_at_ms = Some(now);
            record.updated_at_ms = record.updated_at_ms.max(now);
            record.last_error =
                Some("gateway restarted before Runtime acknowledged the trigger event".to_string());
        }
        if is_terminal_trigger_event_status(&record.status) {
            record.next_retry_at_ms = None;
        }
        if record.attempts > record.max_attempts {
            record.attempts = record.max_attempts;
        }
    }
}

fn normalize_terminal_outbox_state(state: &mut SurfaceMessageState) {
    let now = now_ms();
    for record in state.outbox.values_mut() {
        if record.status == "sending" {
            record.status = "retry_scheduled".to_string();
            record.next_retry_at_ms = Some(now);
            record.claim_owner = None;
            record.lease_until_ms = None;
            record.updated_at_ms = record.updated_at_ms.max(now);
            record.last_error =
                Some("gateway restarted before the outbound delivery claim completed".to_string());
        }
        if is_terminal_outbox_status(&record.status) {
            record.next_retry_at_ms = None;
            record.claim_owner = None;
            record.lease_until_ms = None;
        }
        if record.status == "dead_letter" && record.attempts > record.max_attempts {
            record.attempts = record.max_attempts;
        }
    }
}

fn reconcile_inbox_with_outbox(state: &mut SurfaceMessageState) {
    for inbox in state.inbox.values_mut() {
        let related = state.outbox.values().filter(|outbox| {
            outbox.surface == inbox.surface
                && outbox
                    .reply_to_message_id
                    .as_deref()
                    .is_some_and(|message_id| message_id == inbox.message_id)
        });
        let mut sent = None;
        let mut failed = None;
        let mut active = None;
        for outbox in related {
            match outbox.status.as_str() {
                "sent" => sent = Some(outbox),
                "dead_letter" => failed = Some(outbox),
                "queued" | "sending" | "retry_scheduled" => active = Some(outbox),
                _ => {}
            }
        }
        if let Some(outbox) = sent {
            inbox.status = if outbox_is_failure_notice(outbox) {
                "failed_notified"
            } else {
                "replied"
            }
            .to_string();
            inbox.updated_at_ms = inbox.updated_at_ms.max(outbox.updated_at_ms);
            inbox.last_error = outbox_failure_reason(outbox);
        } else if let Some(outbox) = failed {
            inbox.status = "reply_failed".to_string();
            inbox.updated_at_ms = inbox.updated_at_ms.max(outbox.updated_at_ms);
            inbox.last_error = outbox.last_error.clone();
        } else if let Some(outbox) = active {
            inbox.status = if outbox.status == "retry_scheduled" {
                "reply_retry_scheduled"
            } else if outbox_is_failure_notice(outbox) {
                "failure_notifying"
            } else {
                "replying"
            }
            .to_string();
            inbox.updated_at_ms = inbox.updated_at_ms.max(outbox.updated_at_ms);
            inbox.last_error = outbox
                .last_error
                .clone()
                .or_else(|| outbox_failure_reason(outbox));
        } else if is_active_inbox_status(&inbox.status) && inbox.correlation.is_none() {
            inbox.status = "failed".to_string();
            inbox.updated_at_ms = now_ms();
            inbox.last_error = Some(
                "surface processing was interrupted by gateway restart before a reply was queued"
                    .to_string(),
            );
        }
    }
}

fn is_active_inbox_status(status: &str) -> bool {
    matches!(
        status,
        "received"
            | "processing"
            | "processed"
            | "replying"
            | "failure_notifying"
            | "reply_retry_scheduled"
    )
}

fn is_active_outbox_status(status: &str) -> bool {
    matches!(status, "queued" | "sending" | "retry_scheduled")
}

fn is_terminal_outbox_status(status: &str) -> bool {
    matches!(status, "sent" | "dead_letter" | "cancelled" | "archived")
}

fn is_active_trigger_event_status(status: &str) -> bool {
    matches!(status, "received" | "dispatching" | "retry_scheduled")
}

fn is_terminal_trigger_event_status(status: &str) -> bool {
    matches!(status, "accepted" | "dead_letter")
}

fn read_latest<T>(
    path: PathBuf,
    key_fn: impl Fn(&T) -> String,
) -> Result<BTreeMap<String, T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    let Ok(file) = std::fs::File::open(path) else {
        return Ok(BTreeMap::new());
    };
    let reader = std::io::BufReader::new(file);
    let mut records = BTreeMap::new();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<T>(&line).map_err(|error| error.to_string())?;
        records.insert(key_fn(&record), record);
    }
    Ok(records)
}

fn write_jsonl_atomic<'a, T: Serialize + 'a>(
    path: PathBuf,
    records: impl IntoIterator<Item = &'a T>,
) -> Result<(), String> {
    let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let mut writer = fs::File::create(&temporary).map_err(|error| error.to_string())?;
    for record in records {
        serde_json::to_writer(&mut writer, record).map_err(|error| error.to_string())?;
        writer.write_all(b"\n").map_err(|error| error.to_string())?;
    }
    writer.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, &path).map_err(|error| error.to_string())
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn next_retry_at_ms(attempts: u32) -> i64 {
    let exponent = attempts.saturating_sub(1).min(6);
    let delay_secs = 2_i64.pow(exponent).min(60);
    (Utc::now() + ChronoDuration::seconds(delay_secs)).timestamp_millis()
}

fn new_event_id() -> String {
    format!("surface-delivery-event-{}", uuid::Uuid::new_v4())
}

fn hash_json(value: &serde_json::Value) -> String {
    hash_str(&serde_json::to_string(value).unwrap_or_default())
}

fn hash_str(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn summarize_json(value: &serde_json::Value, limit: usize) -> String {
    summarize_text(&serde_json::to_string(value).unwrap_or_default(), limit)
}

fn summarize_text(value: &str, limit: usize) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= limit {
        compact
    } else {
        format!(
            "{}...",
            compact
                .chars()
                .take(limit.saturating_sub(3))
                .collect::<String>()
        )
    }
}

fn outbox_is_failure_notice(record: &SurfaceOutboxRecord) -> bool {
    record
        .request_json
        .get("metadata")
        .and_then(|metadata| metadata.get("failure_notice"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn outbox_failure_reason(record: &SurfaceOutboxRecord) -> Option<String> {
    record
        .request_json
        .get("metadata")
        .and_then(|metadata| metadata.get("failure_reason"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn session_projection(phase: &str) -> SurfaceSessionProjectionDraft {
        SurfaceSessionProjectionDraft {
            phase: phase.to_string(),
            session_id: "surface:feishu:sender".to_string(),
            scope: "message".to_string(),
            kind: format!("surface.message_{phase}"),
            status: phase.to_string(),
            payload_json: serde_json::json!({"type": phase, "message_id": "msg-1"}),
            phase_offset_ms: 0,
        }
    }

    #[test]
    fn surface_message_store_rejects_non_sqlite_storage_endpoint() {
        let endpoint = storage::StorageEndpoint::postgres(
            storage::StorageDomainId::SurfaceMessages,
            storage::StorageScope::Global,
            "surface",
            "surface_messages_postgres_test",
        );

        let error = SqliteSurfaceMessageStore::from_storage_endpoint(&endpoint)
            .expect_err("non-SQLite endpoint must fail closed");

        assert!(error.contains("require a sqlite endpoint"));
    }

    fn trigger_event(id: &str) -> ManagedAgentTriggerEvent {
        ManagedAgentTriggerEvent {
            event_id: format!("surface-event:feishu:message.received:{id}"),
            source_id: "feishu".to_string(),
            source_kind: "surface".to_string(),
            event_type: "message.received".to_string(),
            subject: "surface:feishu:chat-1".to_string(),
            payload_ref: format!("surface-event:{id}"),
            payload_digest: "sha256:test".to_string(),
            occurred_at_ms: 1,
            source_sequence: None,
            idempotency_key: format!("surface-event:feishu:message.received:{id}"),
            source_capabilities: vec!["surface.event.receive".to_string()],
            attributes: BTreeMap::new(),
            trace_refs: vec![format!("surface:feishu:event:{id}")],
        }
    }

    #[test]
    fn trigger_event_handoff_recovers_an_unacknowledged_runtime_delivery() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-trigger-event-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let event = trigger_event("event-1");
        let received = store
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &event,
                &serde_json::json!({"message_id": "event-1", "text": "hello"}),
            )
            .unwrap();
        assert!(!received.duplicate);
        let claimed = store
            .mark_trigger_event_dispatching(&received.record.idempotency_key)
            .unwrap()
            .expect("fresh event must be claimable");
        assert_eq!(claimed.status, "dispatching");
        assert_eq!(claimed.attempts, 1);

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        let recovered = reloaded.due_trigger_event_retries();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, "retry_scheduled");
        let retried = reloaded
            .mark_trigger_event_dispatching(&received.record.idempotency_key)
            .unwrap()
            .expect("recovered event must be re-claimable");
        assert_eq!(retried.attempts, 2);
        reloaded
            .mark_trigger_event_accepted(&received.record.idempotency_key)
            .unwrap();

        let duplicate = reloaded
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &event,
                &serde_json::json!({"message_id": "event-1", "text": "hello"}),
            )
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.record.status, "accepted");
        let snapshot = reloaded.snapshot("feishu");
        assert!(snapshot.active_trigger_events.is_empty());
        assert!(snapshot.failed_trigger_events.is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn trigger_event_delivery_has_a_bounded_dead_letter_terminal_state() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-trigger-event-dead-letter-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let event = trigger_event("event-2");
        let received = store
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &event,
                &serde_json::json!({"message_id": "event-2"}),
            )
            .unwrap();
        let key = received.record.idempotency_key;
        let mut terminal = None;
        for _ in 0..DEFAULT_MAX_ATTEMPTS {
            store
                .mark_trigger_event_dispatching(&key)
                .unwrap()
                .expect("retryable event must be claimable");
            terminal = Some(
                store
                    .mark_trigger_event_failed(&key, "runtime unavailable")
                    .unwrap(),
            );
            if terminal
                .as_ref()
                .is_some_and(|record| record.status == "dead_letter")
            {
                break;
            }
            let mut state = store.lock_state().unwrap();
            state
                .trigger_events
                .get_mut(&key)
                .expect("record")
                .next_retry_at_ms = Some(now_ms());
        }
        let terminal = terminal.expect("terminal record");
        assert_eq!(terminal.status, "dead_letter");
        assert_eq!(terminal.attempts, DEFAULT_MAX_ATTEMPTS);
        assert!(store.due_trigger_event_retries().is_empty());
        assert_eq!(store.snapshot("feishu").failed_trigger_events.len(), 1);
        let retried = store.retry_trigger_event("feishu", &key).unwrap();
        assert_eq!(retried.status, "received");
        assert_eq!(retried.attempts, 0);
        assert!(store
            .mark_trigger_event_dispatching(&key)
            .unwrap()
            .is_some());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_inbox_dedupes_across_store_reload() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-inbox-store-{}", uuid::Uuid::new_v4()));
        let store = SqliteSurfaceMessageStore::new(&root);
        let received_projection = session_projection("received");
        let receipt = store
            .record_inbox_received(
                "Lark",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "surface:feishu:sender",
                Some("thread-1".to_string()),
                Some("user-1".to_string()),
                std::slice::from_ref(&received_projection),
            )
            .unwrap();
        assert!(!receipt.duplicate);
        let projection = receipt
            .record
            .session_projections
            .first()
            .expect("projection is staged");
        assert_eq!(projection.projection_state, "pending");
        let event_id = projection.event_id.clone();
        store
            .mark_inbox_projection_applied(&receipt.record.idempotency_key, &event_id, 42)
            .unwrap();

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        let duplicate = reloaded
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "surface:feishu:sender",
                Some("thread-1".to_string()),
                Some("user-1".to_string()),
                std::slice::from_ref(&received_projection),
            )
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(reloaded.list_inbox("feishu").len(), 1);
        let projection = &duplicate.record.session_projections[0];
        assert_eq!(projection.event_id, event_id);
        assert_eq!(projection.projection_state, "applied");
        assert_eq!(projection.projected_at_ms, Some(42));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_outbox_records_retry_and_dead_letter_states() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-outbox-store-{}", uuid::Uuid::new_v4()));
        let store = SqliteSurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: Some("thread-1".to_string()),
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({"reply_to": "msg-1"}),
        };
        let queued = store
            .queue_outbox(
                &request,
                Some("session-1".to_string()),
                Some("msg-1".to_string()),
            )
            .unwrap();
        assert_eq!(queued.status, "queued");
        let sending = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        assert_eq!(sending.attempts, 1);
        let retry = store
            .mark_delivery_failed(&queued.delivery_id, "transport timeout", true)
            .unwrap();
        assert_eq!(retry.status, "retry_scheduled");
        let dead = store
            .mark_delivery_dead_letter(&queued.delivery_id, "operator closed")
            .unwrap();
        assert_eq!(dead.status, "dead_letter");
        let sending_after_terminal = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        assert_eq!(sending_after_terminal.status, "dead_letter");
        assert_eq!(sending_after_terminal.attempts, dead.attempts);
        assert_eq!(store.snapshot("feishu").dead_letters.len(), 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn caller_owned_idempotency_key_survives_payload_replay() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-idempotent-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let mut request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "first attempt".to_string(),
            idempotency_key: Some("cross-plane:stable-send".to_string()),
            metadata: serde_json::json!({}),
        };
        let first = store.queue_outbox(&request, None, None).unwrap();
        request.text = "retry after crash".to_string();
        let replay = store.queue_outbox(&request, None, None).unwrap();
        assert_eq!(first.delivery_id, replay.delivery_id);
        assert_eq!(first.idempotency_key, "cross-plane:stable-send");
    }

    #[test]
    fn surface_retry_dead_letter_requires_operator_action() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-retry-store-{}", uuid::Uuid::new_v4()));
        let store = SqliteSurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({}),
        };
        let queued = store.queue_outbox(&request, None, None).unwrap();
        let active_retry = store.mark_delivery_replayed(&queued.delivery_id);
        assert!(active_retry.is_err());

        let _ = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        let _ = store
            .mark_delivery_failed(&queued.delivery_id, "transport timeout", false)
            .unwrap();
        let replayed = store.mark_delivery_replayed(&queued.delivery_id).unwrap();
        assert_eq!(replayed.status, "queued");
        assert_eq!(replayed.attempts, 0);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_archive_dead_letters_preserves_audit_state() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-archive-store-{}", uuid::Uuid::new_v4()));
        let store = SqliteSurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({}),
        };
        let queued = store.queue_outbox(&request, None, None).unwrap();
        let _ = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        let _ = store
            .mark_delivery_failed(&queued.delivery_id, "permanent failure", false)
            .unwrap();
        let archived = store.archive_dead_letters("feishu", None, 10).unwrap();
        assert_eq!(archived.len(), 1);
        let snapshot = store.snapshot("feishu");
        assert!(snapshot.dead_letters.is_empty());
        assert_eq!(snapshot.archived_count, 1);
        assert_eq!(snapshot.archived_outbox[0].status, "archived");
        assert!(snapshot
            .deliveries
            .iter()
            .any(|event| event.kind == "outbox.dead_letter_archived"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_inbox_reaches_replied_after_reply_delivery() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-replied-store-{}", uuid::Uuid::new_v4()));
        let store = SqliteSurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "feishu:user:chat",
                Some("chat".to_string()),
                Some("user".to_string()),
                &[],
            )
            .unwrap();
        store
            .mark_inbox_processing(&inbox.record.idempotency_key, &[])
            .unwrap();
        store
            .mark_inbox_processed(&inbox.record.idempotency_key, Some("turn-1".to_string()))
            .unwrap();
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "chat".to_string(),
            thread: Some("chat".to_string()),
            text: "reply".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({"reply_to": "msg-1"}),
        };
        let delivery = store
            .queue_outbox(
                &request,
                Some("feishu:user:chat".to_string()),
                Some("msg-1".to_string()),
            )
            .unwrap();
        store.mark_delivery_sending(&delivery.delivery_id).unwrap();
        store
            .mark_delivery_sent(
                &delivery.delivery_id,
                &SurfaceOperationResult::ok(
                    "feishu",
                    serde_json::json!({"status": "sent", "message_id": "reply-1"}),
                ),
            )
            .unwrap();

        let snapshot = store.snapshot("feishu");
        assert_eq!(snapshot.inbox[0].status, "replied");
        assert!(snapshot.active_inbox.is_empty());
        assert_eq!(snapshot.terminal_inbox.len(), 1);

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        assert_eq!(reloaded.snapshot("feishu").inbox[0].status, "replied");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn admitted_surface_turn_correlation_survives_reload_and_terminal_replay() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-surface-correlation-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "wecom",
                "message-1",
                &serde_json::json!({"text": "investigate"}),
                "surface:wecom:chat-1",
                Some("thread-1".to_string()),
                Some("user-1".to_string()),
                &[],
            )
            .unwrap();
        let correlation = SurfaceTurnCorrelation {
            surface: "wecom".to_string(),
            message_id: "message-1".to_string(),
            inbox_idempotency_key: inbox.record.idempotency_key.clone(),
            session_id: "surface:wecom:chat-1".to_string(),
            turn_id: "surface-turn:abc".to_string(),
            execution_id: "session-ingress:abc".to_string(),
            reply_to_message_id: "message-1".to_string(),
            reply_idempotency_key: "surface-reply:wecom:message-1".to_string(),
            terminal_id: None,
            terminal_delivery_revision: 0,
        };
        store
            .mark_inbox_admitted(&inbox.record.idempotency_key, correlation, &[])
            .unwrap();
        store
            .record_inbox_terminal_delivery(&inbox.record.idempotency_key, "turn-terminal:req-1")
            .unwrap();
        // Re-observing the same terminal must not create a second recovery
        // revision or a second delivery identity.
        store
            .record_inbox_terminal_delivery(&inbox.record.idempotency_key, "turn-terminal:req-1")
            .unwrap();

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        let record = reloaded
            .get_inbox_message("wecom", "message-1")
            .expect("correlation must be durable");
        let correlation = record.correlation.expect("canonical correlation");
        assert_eq!(record.status, "processed");
        assert_eq!(correlation.execution_id, "session-ingress:abc");
        assert_eq!(
            correlation.terminal_id.as_deref(),
            Some("turn-terminal:req-1")
        );
        assert_eq!(correlation.terminal_delivery_revision, 1);
        assert_eq!(
            correlation.reply_idempotency_key,
            "surface-reply:wecom:message-1"
        );
        assert!(reloaded
            .snapshot("wecom")
            .active_inbox
            .iter()
            .any(|item| item.message_id == "message-1"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_failure_notice_marks_inbox_failed_notified() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-failure-notice-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "inspect readme"}),
                "feishu:user:chat",
                Some("chat".to_string()),
                Some("user".to_string()),
                &[],
            )
            .unwrap();
        store
            .mark_inbox_processing(&inbox.record.idempotency_key, &[])
            .unwrap();
        store
            .mark_inbox_failed(&inbox.record.idempotency_key, "turn timed out after 240s")
            .unwrap();
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "chat".to_string(),
            thread: Some("chat".to_string()),
            text: "failed".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({
                "reply_to": "msg-1",
                "failure_notice": true,
                "failure_reason": "turn timed out after 240s"
            }),
        };
        let delivery = store
            .queue_outbox(
                &request,
                Some("feishu:user:chat".to_string()),
                Some("msg-1".to_string()),
            )
            .unwrap();
        store.mark_delivery_sending(&delivery.delivery_id).unwrap();
        assert_eq!(
            store.snapshot("feishu").inbox[0].status,
            "failure_notifying"
        );
        store
            .mark_delivery_sent(
                &delivery.delivery_id,
                &SurfaceOperationResult::ok(
                    "feishu",
                    serde_json::json!({"status": "sent", "message_id": "reply-1"}),
                ),
            )
            .unwrap();

        let snapshot = store.snapshot("feishu");
        assert_eq!(snapshot.inbox[0].status, "failed_notified");
        assert_eq!(
            snapshot.inbox[0].last_error.as_deref(),
            Some("turn timed out after 240s")
        );
        assert!(snapshot.active_inbox.is_empty());
        assert_eq!(snapshot.terminal_inbox.len(), 1);

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        assert_eq!(
            reloaded.snapshot("feishu").inbox[0].status,
            "failed_notified"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reload_marks_orphan_active_inbox_as_failed() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-orphan-inbox-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "feishu:user:chat",
                Some("chat".to_string()),
                Some("user".to_string()),
                &[],
            )
            .unwrap();
        store
            .mark_inbox_processing(&inbox.record.idempotency_key, &[])
            .unwrap();

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        let snapshot = reloaded.snapshot("feishu");
        assert_eq!(snapshot.inbox[0].status, "failed");
        assert!(snapshot.active_inbox.is_empty());
        assert_eq!(
            snapshot.inbox[0].last_error.as_deref(),
            Some("surface processing was interrupted by gateway restart before a reply was queued")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_purge_archived_events_keeps_outbox_terminal_state() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-purge-store-{}", uuid::Uuid::new_v4()));
        let store = SqliteSurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({}),
        };
        let queued = store.queue_outbox(&request, None, None).unwrap();
        let _ = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        let _ = store
            .mark_delivery_failed(&queued.delivery_id, "transport timeout", false)
            .unwrap();
        let archived = store.archive_dead_letters("feishu", None, 10).unwrap();
        assert_eq!(archived.len(), 1);
        assert!(!store.list_delivery_events("feishu").is_empty());

        let purged = store.purge_archived_events("feishu", None, 100).unwrap();
        assert!(purged > 0);
        let snapshot = store.snapshot("feishu");
        assert_eq!(snapshot.archived_count, 1);
        assert_eq!(snapshot.archived_outbox[0].delivery_id, queued.delivery_id);
        assert!(store
            .list_delivery_events("feishu")
            .iter()
            .all(|event| event.delivery_id.as_deref() != Some(&queued.delivery_id)));

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        assert_eq!(reloaded.snapshot("feishu").archived_count, 1);
        assert!(reloaded
            .list_delivery_events("feishu")
            .iter()
            .all(|event| event.delivery_id.as_deref() != Some(&queued.delivery_id)));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_ingress_claims_one_frame_per_session_and_recovers_after_restart() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-ingress-claim-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let frame = |session: &str, message: &str| SurfaceFrame::Event {
            surface: "feishu".to_string(),
            event: "message.received".to_string(),
            payload: serde_json::json!({
                "session_id": session,
                "message_id": message,
                "text": "fixture"
            }),
        };
        let first_key = store
            .persist_ingress_frame(&frame("session-a", "a-1"))
            .unwrap();
        store
            .persist_ingress_frame(&frame("session-a", "a-2"))
            .unwrap();
        store
            .persist_ingress_frame(&frame("session-b", "b-1"))
            .unwrap();

        let first_claim = store.claim_ingress_frames("worker-1", 8, 300_000).unwrap();
        assert_eq!(first_claim.len(), 2);
        assert_eq!(
            first_claim
                .iter()
                .filter_map(|claim| match &claim.frame {
                    SurfaceFrame::Event { payload, .. } => payload["session_id"].as_str(),
                    _ => None,
                })
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            2
        );
        store.complete_ingress_frame(&first_key).unwrap();

        let reloaded = SqliteSurfaceMessageStore::new(&root);
        let recovered = reloaded
            .claim_ingress_frames("worker-2", 8, 300_000)
            .unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(
            recovered
                .iter()
                .filter_map(|claim| match &claim.frame {
                    SurfaceFrame::Event { payload, .. } => payload["session_id"].as_str(),
                    _ => None,
                })
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            2
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn production_mutations_write_sqlite_without_jsonl_dual_write() {
        let root =
            std::env::temp_dir().join(format!("cowd-surface-sqlite-only-{}", uuid::Uuid::new_v4()));
        let store = SqliteSurfaceMessageStore::new(&root);
        store
            .record_inbox_received(
                "feishu",
                "message-1",
                &serde_json::json!({"text": "hello"}),
                "session-1",
                None,
                None,
                &[],
            )
            .unwrap();

        assert!(root.join(DATABASE_FILE).exists());
        assert!(!root.join(INBOX_FILE).exists());
        assert!(!root.join(OUTBOX_FILE).exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_jsonl_import_records_count_hash_and_supports_atomic_reverse_export() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-legacy-import-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let record = SurfaceInboxRecord {
            id: "feishu:legacy-1".to_string(),
            surface: "feishu".to_string(),
            message_id: "legacy-1".to_string(),
            idempotency_key: "feishu:legacy-1".to_string(),
            thread_id: None,
            sender_id: None,
            payload_hash: "legacy-hash".to_string(),
            payload_summary: "legacy".to_string(),
            payload_json: serde_json::json!({"text": "legacy"}),
            status: "received".to_string(),
            received_at_ms: 1,
            updated_at_ms: 1,
            runtime_session_id: Some("legacy-session".to_string()),
            runtime_turn_id: None,
            correlation: None,
            session_projections: Vec::new(),
            last_error: None,
        };
        std::fs::write(
            root.join(INBOX_FILE),
            format!("{}\n", serde_json::to_string(&record).unwrap()),
        )
        .unwrap();

        let store = SqliteSurfaceMessageStore::new(&root);
        assert_eq!(store.list_inbox("feishu").len(), 1);
        let evidence = store
            .executor
            .checkout()
            .unwrap()
            .query_row(
                "SELECT value FROM surface_store_meta WHERE key='legacy_jsonl_import_v1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        let evidence: serde_json::Value = serde_json::from_str(&evidence).unwrap();
        assert_eq!(evidence["inbox"], 1);
        assert!(evidence["sha256"]
            .as_str()
            .is_some_and(|hash| hash.len() == 64));

        let export = root.join("reverse-export");
        store.export_legacy_jsonl(&export).unwrap();
        assert_eq!(
            read_latest::<SurfaceInboxRecord>(export.join(INBOX_FILE), |record| record
                .idempotency_key
                .clone())
            .unwrap()
            .len(),
            1
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_outbox_idempotency_creates_one_durable_delivery_identity() {
        let root =
            std::env::temp_dir().join(format!("cowd-surface-outbox-race-{}", uuid::Uuid::new_v4()));
        let store = Arc::new(SqliteSurfaceMessageStore::new(&root));
        let mut workers = Vec::new();
        for _ in 0..32 {
            let store = store.clone();
            workers.push(std::thread::spawn(move || {
                store
                    .queue_outbox(
                        &SurfaceSendRequest {
                            surface: "feishu".to_string(),
                            recipient: "user-1".to_string(),
                            thread: None,
                            text: "one logical reply".to_string(),
                            idempotency_key: Some("reply:stable-1".to_string()),
                            metadata: serde_json::Value::Null,
                        },
                        None,
                        None,
                    )
                    .unwrap()
                    .delivery_id
            }));
        }
        let delivery_ids = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(delivery_ids.len(), 1);
        assert_eq!(store.list_outbox("feishu").len(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_ingress_burst_drains_in_bounded_claim_batches() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-ingress-burst-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteSurfaceMessageStore::new(&root);
        let started = std::time::Instant::now();
        for index in 0..320 {
            store
                .persist_ingress_frame(&SurfaceFrame::Event {
                    surface: "feishu".to_string(),
                    event: "message.received".to_string(),
                    payload: serde_json::json!({
                        "session_id": format!("session-{index}"),
                        "message_id": format!("message-{index}"),
                        "text": "fixture"
                    }),
                })
                .unwrap();
        }
        let mut drained = 0usize;
        loop {
            let claims = store
                .claim_ingress_frames("burst-worker", 32, 30_000)
                .unwrap();
            if claims.is_empty() {
                break;
            }
            assert!(claims.len() <= 32);
            for claim in claims {
                store.complete_ingress_frame(&claim.record_key).unwrap();
                drained += 1;
            }
        }
        let elapsed = started.elapsed();
        eprintln!(
            "surface_ingress_burst records=320 elapsed_ms={}",
            elapsed.as_micros() as f64 / 1_000.0
        );
        assert_eq!(drained, 320);
        assert!(elapsed < std::time::Duration::from_secs(5));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn outbound_claim_is_exclusive_and_expired_lease_returns_to_retry_queue() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-outbox-lease-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Arc::new(SqliteSurfaceMessageStore::new(&root));
        let queued = store
            .queue_outbox(
                &SurfaceSendRequest {
                    surface: "feishu".to_string(),
                    recipient: "user-1".to_string(),
                    thread: None,
                    text: "lease fixture".to_string(),
                    idempotency_key: Some("lease-fixture".to_string()),
                    metadata: serde_json::Value::Null,
                },
                None,
                None,
            )
            .unwrap();
        let mut workers = Vec::new();
        for _ in 0..16 {
            let store = store.clone();
            let delivery_id = queued.delivery_id.clone();
            workers.push(std::thread::spawn(move || {
                store.mark_delivery_sending(&delivery_id).is_ok()
            }));
        }
        assert_eq!(
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .filter(|claimed| *claimed)
                .count(),
            1
        );
        store
            .update_outbox_by_delivery(&queued.delivery_id, |record| {
                record.lease_until_ms = Some(now_ms().saturating_sub(1));
            })
            .unwrap();
        let retry = store.due_retry_deliveries();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].status, "retry_scheduled");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn migration_snapshot_preserves_every_surface_ledger_family() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-migration-source-{}",
            uuid::Uuid::new_v4()
        ));
        let source = SqliteSurfaceMessageStore::new(&root);
        source
            .persist_ingress_frame(&SurfaceFrame::Event {
                surface: "feishu".to_string(),
                event: "message.received".to_string(),
                payload: serde_json::json!({
                    "session_id": "migration-session",
                    "message_id": "migration-message",
                    "text": "migrate every durable family"
                }),
            })
            .unwrap();
        source
            .record_inbox_received(
                "feishu",
                "migration-message",
                &serde_json::json!({"text": "inbox"}),
                "migration-session",
                None,
                None,
                &[],
            )
            .unwrap();
        source
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &trigger_event("migration-trigger"),
                &serde_json::json!({"text": "trigger"}),
            )
            .unwrap();
        source
            .queue_outbox(
                &SurfaceSendRequest {
                    surface: "feishu".to_string(),
                    recipient: "migration-recipient".to_string(),
                    thread: None,
                    text: "outbox".to_string(),
                    idempotency_key: Some("migration-outbox".to_string()),
                    metadata: serde_json::Value::Null,
                },
                Some("migration-session".to_string()),
                Some("migration-message".to_string()),
            )
            .unwrap();

        let snapshot = source.export_migration_snapshot().unwrap();
        assert_eq!(snapshot.inbox.len(), 1);
        assert_eq!(snapshot.outbox.len(), 1);
        assert_eq!(snapshot.trigger_events.len(), 1);
        assert_eq!(snapshot.delivery_events.len(), 3);
        assert_eq!(snapshot.ingress_frames.len(), 1);
        let source_digest = snapshot.canonical_digest().unwrap();

        let target_root = std::env::temp_dir().join(format!(
            "cowd-surface-migration-target-{}",
            uuid::Uuid::new_v4()
        ));
        let target = SqliteSurfaceMessageStore::new(&target_root);
        target.import_migration_snapshot(&snapshot).unwrap();
        assert_eq!(
            target
                .export_migration_snapshot()
                .unwrap()
                .canonical_digest()
                .unwrap(),
            source_digest
        );
        assert!(target.import_migration_snapshot(&snapshot).is_err());
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(target_root);
    }

    /// This is intentionally an opt-in integration test: it proves that the
    /// SQLite implementation exported by the Gateway can be copied into the
    /// PostgreSQL adapter without either backend knowing about the other.
    /// The caller supplies an isolated disposable database through
    /// `COWD_TEST_POSTGRES_URL`.
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn sqlite_snapshot_copies_to_postgres_with_exact_digest() {
        let url = std::env::var("COWD_TEST_POSTGRES_URL")
            .expect("COWD_TEST_POSTGRES_URL is required for PostgreSQL integration tests");
        let resolver =
            storage::StaticSecretRefResolver::new([("surface-postgres-test".into(), url)]);
        let target = surface_postgres::PostgresSurfaceMessageLedger::connect(
            storage::PostgresConnectionConfig::new(
                "surface-sqlite-source-contract",
                "surface-postgres-test",
                "cowd-surface-sqlite-source-contract",
            ),
            &resolver,
        )
        .unwrap();
        target
            .executor()
            .checkout_critical()
            .unwrap()
            .batch_execute(
                "TRUNCATE TABLE surface_delivery_event, surface_ingress_frame, surface_outbox, \
                 surface_trigger_event, surface_inbox",
            )
            .unwrap();

        let root = std::env::temp_dir().join(format!(
            "cowd-surface-sqlite-source-contract-{}",
            uuid::Uuid::new_v4()
        ));
        let source = SqliteSurfaceMessageStore::new(&root);
        source
            .persist_ingress_frame(&SurfaceFrame::Event {
                surface: "feishu".to_string(),
                event: "message.received".to_string(),
                payload: serde_json::json!({
                    "session_id": "postgres-migration-session",
                    "message_id": "postgres-migration-message",
                    "text": "copy every durable family"
                }),
            })
            .unwrap();
        source
            .record_inbox_received(
                "feishu",
                "postgres-migration-message",
                &serde_json::json!({"text": "inbox"}),
                "postgres-migration-session",
                None,
                None,
                &[],
            )
            .unwrap();
        source
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &trigger_event("postgres-migration-trigger"),
                &serde_json::json!({"text": "trigger"}),
            )
            .unwrap();
        source
            .queue_outbox(
                &SurfaceSendRequest {
                    surface: "feishu".to_string(),
                    recipient: "postgres-migration-recipient".to_string(),
                    thread: None,
                    text: "outbox".to_string(),
                    idempotency_key: Some("postgres-migration-outbox".to_string()),
                    metadata: serde_json::Value::Null,
                },
                Some("postgres-migration-session".to_string()),
                Some("postgres-migration-message".to_string()),
            )
            .unwrap();

        let manifest = surface_postgres::copy_quiesced_surface_message_ledger(
            &source,
            &target,
            root.join("surface-message-migration-manifest.json"),
        )
        .unwrap();
        assert_eq!(manifest.source_digest, manifest.target_digest);
        assert_eq!(manifest.inbox_count, 1);
        assert_eq!(manifest.outbox_count, 1);
        assert_eq!(manifest.trigger_event_count, 1);
        assert_eq!(manifest.ingress_frame_count, 1);
        assert!(target
            .import_migration_snapshot(&source.export_migration_snapshot().unwrap())
            .is_err());
        let _ = std::fs::remove_dir_all(root);
    }
}
