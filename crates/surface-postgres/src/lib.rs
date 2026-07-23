#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
//! PostgreSQL adapter for the Surface durable message ledger.
//!
//! The adapter deliberately owns SQL, row locking and PostgreSQL migration
//! state. `surface` owns the DTOs and behaviour contract; neither it nor
//! Gateway needs a PostgreSQL driver dependency.

use std::fs;
use std::path::{Path, PathBuf};

use postgres::Row;
use serde_json::Value;
use sha2::{Digest, Sha256};
use storage::{
    PostgresClient, PostgresConnectionConfig, PostgresExecutor, PostgresMigrationSpec,
    SecretRefResolver,
};
use surface::{
    normalize_surface_id, SurfaceDeliveryEvent, SurfaceFrame, SurfaceInboxReceipt,
    SurfaceInboxRecord, SurfaceIngressClaim, SurfaceIngressFrameRecord, SurfaceMessageLedger,
    SurfaceMessageLedgerMigrationSnapshot, SurfaceMessageSnapshot, SurfaceOperationResult,
    SurfaceOutboxRecord, SurfaceSendRequest, SurfaceTriggerEventReceipt, SurfaceTriggerEventRecord,
    SurfaceTurnCorrelation,
};

const DOMAIN: &str = "surface_message";
const MAX_ATTEMPTS: u32 = 5;

const MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "surface_message.0001.initial",
    domain: DOMAIN,
    version: 1,
    description: "create durable surface message ledger",
    statements: &[
        "CREATE TABLE IF NOT EXISTS surface_inbox (
            record_key TEXT PRIMARY KEY, surface TEXT NOT NULL, status TEXT NOT NULL,
            next_retry_at_ms BIGINT, updated_at_ms BIGINT NOT NULL, record_json JSONB NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS surface_outbox (
            record_key TEXT PRIMARY KEY, delivery_id TEXT NOT NULL UNIQUE, surface TEXT NOT NULL,
            status TEXT NOT NULL, next_retry_at_ms BIGINT, lease_until_ms BIGINT,
            updated_at_ms BIGINT NOT NULL, record_json JSONB NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS surface_trigger_event (
            record_key TEXT PRIMARY KEY, surface TEXT NOT NULL, status TEXT NOT NULL,
            next_retry_at_ms BIGINT, updated_at_ms BIGINT NOT NULL, record_json JSONB NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS surface_delivery_event (
            record_key TEXT PRIMARY KEY, surface TEXT NOT NULL, status TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL, record_json JSONB NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS surface_ingress_frame (
            record_key TEXT PRIMARY KEY, surface TEXT NOT NULL, session_id TEXT NOT NULL,
            status TEXT NOT NULL, attempts BIGINT NOT NULL, max_attempts BIGINT NOT NULL,
            next_retry_at_ms BIGINT, claim_owner TEXT, lease_until_ms BIGINT,
            created_at_ms BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL,
            frame_json JSONB NOT NULL, last_error TEXT
        )",
        "CREATE INDEX IF NOT EXISTS idx_surface_inbox_surface_status ON surface_inbox(surface, status, updated_at_ms, record_key)",
        "CREATE INDEX IF NOT EXISTS idx_surface_outbox_due ON surface_outbox(status, next_retry_at_ms, lease_until_ms, record_key)",
        "CREATE INDEX IF NOT EXISTS idx_surface_trigger_due ON surface_trigger_event(status, next_retry_at_ms, record_key)",
        "CREATE INDEX IF NOT EXISTS idx_surface_delivery_surface_created ON surface_delivery_event(surface, created_at_ms, record_key)",
        "CREATE INDEX IF NOT EXISTS idx_surface_ingress_claim ON surface_ingress_frame(status, next_retry_at_ms, lease_until_ms, session_id, created_at_ms)",
    ],
}];

#[derive(Clone, Debug)]
pub struct PostgresSurfaceMessageLedger {
    executor: PostgresExecutor,
}

impl PostgresSurfaceMessageLedger {
    pub fn new(executor: PostgresExecutor) -> Result<Self, String> {
        executor
            .apply_migrations(DOMAIN, MIGRATIONS)
            .map_err(|error| error.to_string())?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, String> {
        Self::new(PostgresExecutor::connect(config, resolver).map_err(|error| error.to_string())?)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    fn update_inbox(
        &self,
        key: &str,
        mutate: impl FnOnce(&mut SurfaceInboxRecord) -> Result<(), String>,
        event_kind: &str,
    ) -> Result<SurfaceInboxRecord, String> {
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let mut record = inbox_for_update(&mut tx, key)?;
        mutate(&mut record)?;
        record.updated_at_ms = now_ms();
        store_inbox(&mut tx, &record)?;
        insert_event(
            &mut tx,
            &inbox_event(
                &record,
                event_kind,
                serde_json::json!({
                    "runtime_turn_id": record.runtime_turn_id,
                    "last_error": record.last_error,
                }),
            ),
        )?;
        tx.commit().map_err(stringify)?;
        Ok(record)
    }

    fn update_trigger(
        &self,
        key: &str,
        mutate: impl FnOnce(&mut SurfaceTriggerEventRecord) -> Result<(), String>,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let mut record = trigger_for_update(&mut tx, key)?;
        mutate(&mut record)?;
        record.updated_at_ms = now_ms();
        store_trigger(&mut tx, &record)?;
        insert_event(&mut tx, &trigger_event(&record))?;
        tx.commit().map_err(stringify)?;
        Ok(record)
    }

    /// Changes a delivery, its authoritative state-transition event, and its
    /// related inbound-reply projection in one PostgreSQL transaction. A
    /// crash can therefore never leave `outbox.sent` persisted without the
    /// corresponding event or the inbox's reply state still stale.
    fn update_outbox_with_effects(
        &self,
        delivery_id: &str,
        mutate: impl FnOnce(&mut SurfaceOutboxRecord) -> Result<(), String>,
        event: impl FnOnce(&SurfaceOutboxRecord) -> Option<SurfaceDeliveryEvent>,
        inbox_update: impl FnOnce(&SurfaceOutboxRecord) -> Option<(String, Option<String>)>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let mut record = outbox_for_update(&mut tx, delivery_id)?;
        mutate(&mut record)?;
        record.updated_at_ms = now_ms();
        store_outbox(&mut tx, &record)?;
        if let Some(event) = event(&record) {
            insert_event(&mut tx, &event)?;
        }
        if let (Some(reply), Some((status, error))) =
            (record.reply_to_message_id.as_deref(), inbox_update(&record))
        {
            mark_inbox_by_message_tx(&mut tx, &record.surface, reply, &status, error)?;
        }
        tx.commit().map_err(stringify)?;
        Ok(record)
    }
}

impl SurfaceMessageLedger for PostgresSurfaceMessageLedger {
    fn diagnostic_root(&self) -> PathBuf {
        PathBuf::from(format!(
            "postgres:{}",
            self.executor.health().logical_identity
        ))
    }

    fn persist_ingress_frame(&self, frame: &SurfaceFrame) -> Result<String, String> {
        let SurfaceFrame::Event {
            surface, payload, ..
        } = frame
        else {
            return Err("only Surface event frames can enter durable ingress".to_string());
        };
        let frame_json = serde_json::to_value(frame).map_err(stringify)?;
        let record_key = format!("surface-ingress:{}", hash_json(&frame_json));
        let now = now_ms();
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        connection.execute(
            "INSERT INTO surface_ingress_frame(record_key,surface,session_id,status,attempts,max_attempts,next_retry_at_ms,created_at_ms,updated_at_ms,frame_json)
             VALUES($1,$2,$3,'pending',0,$4,$5,$5,$5,$6) ON CONFLICT(record_key) DO NOTHING",
            &[&record_key, &normalize_surface_id(surface), &surface_session_id(surface, payload), &as_i64(MAX_ATTEMPTS), &now, &frame_json],
        ).map_err(stringify)?;
        Ok(record_key)
    }

    fn claim_ingress_frames(
        &self,
        owner: &str,
        limit: usize,
        lease_ms: i64,
    ) -> Result<Vec<SurfaceIngressClaim>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = now_ms();
        let lease_until = now.saturating_add(lease_ms.max(1));
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        tx.execute("UPDATE surface_ingress_frame SET status='retry_scheduled',claim_owner=NULL,lease_until_ms=NULL,next_retry_at_ms=$1,updated_at_ms=$1,last_error='gateway worker lease expired before durable completion' WHERE status='claimed' AND lease_until_ms <= $1", &[&now]).map_err(stringify)?;
        let rows = tx
            .query(
                "SELECT record_key,session_id,frame_json FROM surface_ingress_frame
             WHERE status IN ('pending','retry_scheduled') AND attempts < max_attempts
               AND (next_retry_at_ms IS NULL OR next_retry_at_ms <= $1)
             ORDER BY created_at_ms,record_key FOR UPDATE SKIP LOCKED LIMIT $2",
                &[&now, &limit_i64(limit.saturating_mul(8).max(limit))],
            )
            .map_err(stringify)?;
        let mut claims = Vec::new();
        for row in rows {
            if claims.len() >= limit {
                break;
            }
            let key: String = row.try_get(0).map_err(stringify)?;
            let session_id: String = row.try_get(1).map_err(stringify)?;
            let lock_key = format!("cowd-surface-ingress-session:{session_id}");
            let locked: bool = tx
                .query_one(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                    &[&lock_key],
                )
                .map_err(stringify)?
                .try_get(0)
                .map_err(stringify)?;
            if !locked {
                continue;
            }
            let active: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM surface_ingress_frame WHERE session_id=$1 AND status='claimed' AND lease_until_ms>$2)", &[&session_id,&now]).map_err(stringify)?.try_get(0).map_err(stringify)?;
            if active {
                continue;
            }
            let changed = tx.execute("UPDATE surface_ingress_frame SET status='claimed',attempts=attempts+1,claim_owner=$1,lease_until_ms=$2,next_retry_at_ms=NULL,updated_at_ms=$3,last_error=NULL WHERE record_key=$4 AND status IN ('pending','retry_scheduled')", &[&owner,&lease_until,&now,&key]).map_err(stringify)?;
            if changed == 1 {
                claims.push(SurfaceIngressClaim {
                    record_key: key,
                    frame: row_json(&row, 2)?,
                });
            }
        }
        tx.commit().map_err(stringify)?;
        Ok(claims)
    }

    fn complete_ingress_frame(&self, key: &str) -> Result<(), String> {
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        connection.execute("UPDATE surface_ingress_frame SET status='completed',claim_owner=NULL,lease_until_ms=NULL,next_retry_at_ms=NULL,updated_at_ms=$1,last_error=NULL WHERE record_key=$2 AND status='claimed'", &[&now_ms(),&key]).map_err(stringify)?;
        Ok(())
    }

    fn fail_ingress_frame(&self, key: &str, error: &str) -> Result<(), String> {
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let row = tx.query_opt("SELECT attempts,max_attempts FROM surface_ingress_frame WHERE record_key=$1 FOR UPDATE", &[&key]).map_err(stringify)?.ok_or_else(|| format!("surface ingress `{key}` not found"))?;
        let attempts = as_u32(row.try_get::<_, i64>(0).map_err(stringify)?, "attempts")?;
        let max = as_u32(row.try_get::<_, i64>(1).map_err(stringify)?, "max_attempts")?;
        let terminal = attempts >= max;
        tx.execute("UPDATE surface_ingress_frame SET status=$1,claim_owner=NULL,lease_until_ms=NULL,next_retry_at_ms=$2,updated_at_ms=$3,last_error=$4 WHERE record_key=$5", &[&if terminal { "dead_letter" } else { "retry_scheduled" }, &if terminal { None } else { Some(next_retry_at_ms(attempts)) }, &now_ms(), &error, &key]).map_err(stringify)?;
        tx.commit().map_err(stringify)
    }

    fn record_inbox_received(
        &self,
        surface: &str,
        message_id: &str,
        payload: &Value,
        session: &str,
        thread: Option<String>,
        sender: Option<String>,
    ) -> Result<SurfaceInboxReceipt, String> {
        let surface = normalize_surface_id(surface);
        let key = inbound_key(&surface, message_id);
        let now = now_ms();
        let record = SurfaceInboxRecord {
            id: key.clone(),
            surface: surface.clone(),
            message_id: message_id.to_string(),
            idempotency_key: key.clone(),
            thread_id: thread,
            sender_id: sender,
            payload_hash: hash_json(payload),
            payload_summary: summarize_json(payload, 240),
            payload_json: payload.clone(),
            status: "received".to_string(),
            received_at_ms: now,
            updated_at_ms: now,
            runtime_session_id: Some(session.to_string()),
            runtime_turn_id: None,
            correlation: None,
            last_error: None,
        };
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let inserted = tx.execute("INSERT INTO surface_inbox(record_key,surface,status,next_retry_at_ms,updated_at_ms,record_json) VALUES($1,$2,$3,NULL,$4,$5) ON CONFLICT(record_key) DO NOTHING", &[&key,&surface,&record.status,&now,&serde_json::to_value(&record).map_err(stringify)?]).map_err(stringify)?;
        if inserted == 0 {
            let existing = inbox_for_update(&mut tx, &key)?;
            tx.commit().map_err(stringify)?;
            return Ok(SurfaceInboxReceipt {
                record: existing,
                duplicate: true,
            });
        }
        insert_event(
            &mut tx,
            &inbox_event(
                &record,
                "received",
                serde_json::json!({"runtime_session_id":session,"payload_summary":record.payload_summary}),
            ),
        )?;
        tx.commit().map_err(stringify)?;
        Ok(SurfaceInboxReceipt {
            record,
            duplicate: false,
        })
    }

    fn mark_inbox_processing(&self, key: &str) -> Result<(), String> {
        self.update_inbox(
            key,
            |r| {
                r.status = "processing".into();
                r.last_error = None;
                Ok(())
            },
            "processing",
        )
        .map(|_| ())
    }
    fn mark_inbox_processed(&self, key: &str, turn: Option<String>) -> Result<(), String> {
        self.update_inbox(
            key,
            |r| {
                r.status = "processed".into();
                if turn.is_some() {
                    r.runtime_turn_id = turn;
                }
                r.last_error = None;
                Ok(())
            },
            "processed",
        )
        .map(|_| ())
    }
    fn mark_inbox_admitted(&self, key: &str, c: SurfaceTurnCorrelation) -> Result<(), String> {
        self.update_inbox(
            key,
            |r| {
                r.status = "processed".into();
                r.runtime_session_id = Some(c.session_id.clone());
                r.runtime_turn_id = Some(c.turn_id.clone());
                r.correlation = Some(c);
                r.last_error = None;
                Ok(())
            },
            "processed",
        )
        .map(|_| ())
    }
    fn record_inbox_terminal_delivery(&self, key: &str, terminal: &str) -> Result<(), String> {
        self.update_inbox(
            key,
            |r| {
                let c = r
                    .correlation
                    .as_mut()
                    .ok_or_else(|| format!("surface inbox `{key}` has no turn correlation"))?;
                if c.terminal_id.as_deref() != Some(terminal) {
                    c.terminal_id = Some(terminal.to_string());
                    c.terminal_delivery_revision = c.terminal_delivery_revision.saturating_add(1);
                }
                Ok(())
            },
            "terminal_delivery_observed",
        )
        .map(|_| ())
    }
    fn mark_inbox_replied(&self, key: &str) -> Result<(), String> {
        self.update_inbox(
            key,
            |r| {
                r.status = "replied".into();
                r.last_error = None;
                Ok(())
            },
            "replied",
        )
        .map(|_| ())
    }
    fn mark_inbox_reply_failed(&self, key: &str, error: &str) -> Result<(), String> {
        self.update_inbox(
            key,
            |r| {
                r.status = "reply_failed".into();
                r.last_error = Some(error.into());
                Ok(())
            },
            "reply_failed",
        )
        .map(|_| ())
    }
    fn mark_inbox_failed(&self, key: &str, error: &str) -> Result<(), String> {
        self.update_inbox(
            key,
            |r| {
                r.status = "failed".into();
                r.last_error = Some(error.into());
                Ok(())
            },
            "failed",
        )
        .map(|_| ())
    }

    fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &harness_contract::managed_agent::ManagedAgentTriggerEvent,
        payload: &Value,
    ) -> Result<SurfaceTriggerEventReceipt, String> {
        let surface = normalize_surface_id(surface);
        let key = trigger.idempotency_key.clone();
        let now = now_ms();
        let record = SurfaceTriggerEventRecord {
            idempotency_key: key.clone(),
            surface,
            event_type: event_type.into(),
            trigger: trigger.clone(),
            payload_json: payload.clone(),
            status: "received".into(),
            attempts: 0,
            max_attempts: MAX_ATTEMPTS,
            next_retry_at_ms: Some(now),
            created_at_ms: now,
            updated_at_ms: now,
            accepted_at_ms: None,
            last_error: None,
        };
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let inserted=tx.execute("INSERT INTO surface_trigger_event(record_key,surface,status,next_retry_at_ms,updated_at_ms,record_json) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(record_key) DO NOTHING", &[&key,&record.surface,&record.status,&record.next_retry_at_ms,&now,&serde_json::to_value(&record).map_err(stringify)?]).map_err(stringify)?;
        if inserted == 0 {
            let existing = trigger_for_update(&mut tx, &key)?;
            tx.commit().map_err(stringify)?;
            return Ok(SurfaceTriggerEventReceipt {
                record: existing,
                duplicate: true,
            });
        }
        insert_event(&mut tx, &trigger_event(&record))?;
        tx.commit().map_err(stringify)?;
        Ok(SurfaceTriggerEventReceipt {
            record,
            duplicate: false,
        })
    }
    fn mark_trigger_event_dispatching(
        &self,
        key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String> {
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let mut record = trigger_for_update(&mut tx, key)?;
        if !matches!(record.status.as_str(), "received" | "retry_scheduled") {
            tx.commit().map_err(stringify)?;
            return Ok(None);
        }
        record.status = "dispatching".into();
        record.attempts = record.attempts.saturating_add(1);
        record.next_retry_at_ms = None;
        record.last_error = None;
        record.updated_at_ms = now_ms();
        store_trigger(&mut tx, &record)?;
        insert_event(&mut tx, &trigger_event(&record))?;
        tx.commit().map_err(stringify)?;
        Ok(Some(record))
    }
    fn mark_trigger_event_accepted(&self, key: &str) -> Result<SurfaceTriggerEventRecord, String> {
        self.update_trigger(key, |r| {
            r.status = "accepted".into();
            r.next_retry_at_ms = None;
            r.accepted_at_ms = Some(now_ms());
            r.last_error = None;
            Ok(())
        })
    }
    fn mark_trigger_event_failed(
        &self,
        key: &str,
        error: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.update_trigger(key, |r| {
            r.last_error = Some(error.into());
            if r.attempts < r.max_attempts {
                r.status = "retry_scheduled".into();
                r.next_retry_at_ms = Some(next_retry_at_ms(r.attempts));
            } else {
                r.status = "dead_letter".into();
                r.next_retry_at_ms = None;
            }
            Ok(())
        })
    }
    fn retry_trigger_event(
        &self,
        surface: &str,
        key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let surface = normalize_surface_id(surface);
        self.update_trigger(key,|r|{if r.surface!=surface{return Err(format!("surface trigger event `{key}` does not belong to surface `{surface}`"))}if r.status!="dead_letter"{return Err(format!("operator retry is only allowed for dead_letter trigger events; current status is {}",r.status))}r.status="received".into();r.attempts=0;r.next_retry_at_ms=Some(now_ms());r.last_error=None;Ok(())})
    }

    fn queue_outbox(
        &self,
        request: &SurfaceSendRequest,
        source: Option<String>,
        reply: Option<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let surface = normalize_surface_id(&request.surface);
        let key = request.idempotency_key.clone().unwrap_or_else(|| {
            outbound_key(
                &surface,
                reply.as_deref(),
                &request.recipient,
                &request.text,
            )
        });
        let now = now_ms();
        let record = SurfaceOutboxRecord {
            delivery_id: format!("surface-delivery-{}", uuid::Uuid::new_v4()),
            surface,
            recipient: request.recipient.clone(),
            thread_id: request.thread.clone(),
            idempotency_key: key.clone(),
            text_hash: hash_str(&request.text),
            text_summary: summarize_text(&request.text, 240),
            request_json: serde_json::to_value(request).map_err(stringify)?,
            status: "queued".into(),
            attempts: 0,
            max_attempts: MAX_ATTEMPTS,
            next_retry_at_ms: None,
            claim_owner: None,
            lease_until_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
            sent_at_ms: None,
            last_error: None,
            source_session_id: source,
            reply_to_message_id: reply,
        };
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let inserted = insert_outbox_if_absent(&mut tx, &record)?;
        if !inserted {
            let existing = outbox_by_key(&mut tx, &key)?;
            tx.commit().map_err(stringify)?;
            return Ok(existing);
        }
        insert_event(
            &mut tx,
            &outbox_event(
                &record,
                "queued",
                serde_json::json!({"recipient":record.recipient,"thread_id":record.thread_id,"text_summary":record.text_summary}),
            ),
        )?;
        tx.commit().map_err(stringify)?;
        Ok(record)
    }
    fn mark_delivery_sending(&self, id: &str) -> Result<SurfaceOutboxRecord, String> {
        self.update_outbox_with_effects(
            id,
            |record| {
                if is_terminal_outbox(&record.status) {
                    return Ok(());
                }
                if !matches!(record.status.as_str(), "queued" | "retry_scheduled") {
                    return Err(format!(
                        "surface delivery is already claimed or terminal ({})",
                        record.status
                    ));
                }
                record.status = "sending".into();
                record.attempts = record.attempts.saturating_add(1);
                record.next_retry_at_ms = None;
                record.claim_owner = Some(format!("surface-delivery:{id}"));
                record.lease_until_ms = Some(now_ms().saturating_add(30_000));
                record.last_error = None;
                Ok(())
            },
            |_| None,
            |record| {
                record.reply_to_message_id.as_ref().map(|_| {
                    (
                        if outbox_failure_notice(record) {
                            "failure_notifying"
                        } else {
                            "replying"
                        }
                        .to_string(),
                        outbox_failure_reason(record),
                    )
                })
            },
        )
    }
    fn mark_delivery_sent(
        &self,
        id: &str,
        result: &SurfaceOperationResult,
    ) -> Result<SurfaceOutboxRecord, String> {
        let result = serde_json::to_value(result).unwrap_or_else(|_| serde_json::json!({}));
        self.update_outbox_with_effects(
            id,
            |r| {
                r.status = "sent".into();
                r.sent_at_ms = Some(now_ms());
                r.next_retry_at_ms = None;
                r.claim_owner = None;
                r.lease_until_ms = None;
                r.last_error = None;
                Ok(())
            },
            |record| Some(outbox_event(record, "sent", result)),
            |record| {
                record.reply_to_message_id.as_ref().map(|_| {
                    (
                        if outbox_failure_notice(record) {
                            "failed_notified"
                        } else {
                            "replied"
                        }
                        .to_string(),
                        outbox_failure_reason(record),
                    )
                })
            },
        )
    }
    fn mark_delivery_failed(
        &self,
        id: &str,
        error: &str,
        retryable: bool,
    ) -> Result<SurfaceOutboxRecord, String> {
        let error = error.to_string();
        self.update_outbox_with_effects(
            id,
            |r| {
                r.last_error = Some(error.into());
                r.claim_owner = None;
                r.lease_until_ms = None;
                if retryable && r.attempts < r.max_attempts {
                    r.status = "retry_scheduled".into();
                    r.next_retry_at_ms = Some(next_retry_at_ms(r.attempts));
                } else {
                    r.status = "dead_letter".into();
                    r.next_retry_at_ms = None;
                }
                Ok(())
            },
            |record| {
                Some(outbox_event(
                    record,
                    if record.status == "dead_letter" {
                        "dead_letter"
                    } else {
                        "retry_scheduled"
                    },
                    serde_json::json!({
                        "attempts": record.attempts,
                        "max_attempts": record.max_attempts,
                        "next_retry_at_ms": record.next_retry_at_ms,
                        "last_error": record.last_error,
                    }),
                ))
            },
            |record| {
                record.reply_to_message_id.as_ref().map(|_| {
                    (
                        if record.status == "dead_letter" {
                            "reply_failed"
                        } else {
                            "reply_retry_scheduled"
                        }
                        .to_string(),
                        record.last_error.clone(),
                    )
                })
            },
        )
    }
    fn mark_delivery_dead_letter(
        &self,
        id: &str,
        reason: &str,
    ) -> Result<SurfaceOutboxRecord, String> {
        let reason = reason.to_string();
        self.update_outbox_with_effects(
            id,
            |record| {
                record.status = "dead_letter".into();
                record.next_retry_at_ms = None;
                record.claim_owner = None;
                record.lease_until_ms = None;
                record.last_error = Some(reason.clone());
                Ok(())
            },
            |record| {
                Some(outbox_event(
                    record,
                    "dead_letter",
                    serde_json::json!({
                        "reason": reason, "attempts": record.attempts,
                    }),
                ))
            },
            |record| {
                record
                    .reply_to_message_id
                    .as_ref()
                    .map(|_| ("reply_failed".to_string(), record.last_error.clone()))
            },
        )
    }
    fn mark_delivery_replayed(&self, id: &str) -> Result<SurfaceOutboxRecord, String> {
        self.update_outbox_with_effects(
            id,
            |record| {
                if record.status != "dead_letter" {
                    return Err(format!("operator retry is only allowed for dead_letter deliveries; current status is {}", record.status));
                }
                record.status = "queued".into();
                record.attempts = 0;
                record.next_retry_at_ms = None;
                record.claim_owner = None;
                record.lease_until_ms = None;
                record.last_error = None;
                Ok(())
            },
            |record| Some(outbox_event(record, "operator_retry_requested", serde_json::json!({
                "attempts": 0, "operator_action": "retry",
            }))),
            |_| None,
        )
    }
    fn archive_dead_letters(
        &self,
        surface: &str,
        older: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let rows = tx
            .query(
                "SELECT delivery_id FROM surface_outbox
                 WHERE surface=$1 AND status='dead_letter'
                   AND ($2::BIGINT IS NULL OR updated_at_ms <= $2)
                 ORDER BY updated_at_ms,record_key FOR UPDATE SKIP LOCKED LIMIT $3",
                &[&surface, &older, &limit_i64(limit.max(1))],
            )
            .map_err(stringify)?;
        let mut archived = Vec::new();
        for row in rows {
            let id: String = row.try_get(0).map_err(stringify)?;
            let mut record = outbox_for_update(&mut tx, &id)?;
            record.status = "archived".into();
            record.next_retry_at_ms = None;
            record.updated_at_ms = now_ms();
            store_outbox(&mut tx, &record)?;
            insert_event(
                &mut tx,
                &outbox_event(
                    &record,
                    "dead_letter_archived",
                    serde_json::json!({
                        "attempts": record.attempts,
                        "max_attempts": record.max_attempts,
                        "last_error": record.last_error,
                    }),
                ),
            )?;
            archived.push(record);
        }
        tx.commit().map_err(stringify)?;
        Ok(archived)
    }
    fn purge_archived_events(
        &self,
        surface: &str,
        older: Option<i64>,
        limit: usize,
    ) -> Result<usize, String> {
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let deleted = connection
            .execute(
                "DELETE FROM surface_delivery_event
                WHERE record_key IN (
                    SELECT event.record_key FROM surface_delivery_event event
                    JOIN surface_outbox outbox
                      ON outbox.delivery_id=event.record_json->>'delivery_id'
                    WHERE event.surface=$1 AND outbox.status='archived'
                      AND ($2::BIGINT IS NULL OR event.created_at_ms <= $2)
                    ORDER BY event.created_at_ms,event.record_key LIMIT $3
                )",
                &[
                    &normalize_surface_id(surface),
                    &older,
                    &limit_i64(limit.max(1)),
                ],
            )
            .map_err(stringify)?;
        Ok(deleted as usize)
    }
    fn get_outbox_by_delivery(&self, id: &str) -> Result<Option<SurfaceOutboxRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        get_outbox(&mut c, id)
    }
    fn due_retry_deliveries(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let now = now_ms();
        let mut connection = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = connection.transaction().map_err(stringify)?;
        let expired = tx
            .query(
                "SELECT delivery_id FROM surface_outbox
                WHERE status='sending' AND lease_until_ms <= $1 FOR UPDATE SKIP LOCKED",
                &[&now],
            )
            .map_err(stringify)?;
        for row in expired {
            let id: String = row.try_get(0).map_err(stringify)?;
            let mut record = outbox_for_update(&mut tx, &id)?;
            record.status = "retry_scheduled".into();
            record.next_retry_at_ms = Some(now);
            record.claim_owner = None;
            record.lease_until_ms = None;
            record.updated_at_ms = now;
            record.last_error = Some("surface outbound delivery claim expired".into());
            store_outbox(&mut tx, &record)?;
        }
        let records = rows_json(
            tx.query(
                "SELECT record_json FROM surface_outbox
                WHERE status='retry_scheduled'
                  AND (record_json->>'attempts')::BIGINT < (record_json->>'max_attempts')::BIGINT
                  AND next_retry_at_ms <= $1 ORDER BY next_retry_at_ms,record_key",
                &[&now],
            )
            .map_err(stringify)?,
        )?;
        tx.commit().map_err(stringify)?;
        Ok(records)
    }
    fn due_trigger_event_retries(&self) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        rows_json(c.query("SELECT record_json FROM surface_trigger_event WHERE status IN ('received','retry_scheduled') AND (record_json->>'attempts')::BIGINT < (record_json->>'max_attempts')::BIGINT AND next_retry_at_ms <= $1 ORDER BY next_retry_at_ms,record_key", &[&now_ms()]).map_err(stringify)?)
    }
    fn get_inbox_message(
        &self,
        surface: &str,
        id: &str,
    ) -> Result<Option<SurfaceInboxRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        let row=c.query_opt("SELECT record_json FROM surface_inbox WHERE surface=$1 AND (record_json->>'message_id'=$2 OR record_key=$2)", &[&normalize_surface_id(surface),&id]).map_err(stringify)?;
        row.map(|r| row_json(&r, 0)).transpose()
    }
    fn list_inbox(&self, surface: &str) -> Result<Vec<SurfaceInboxRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        rows_json(
            c.query(
                "SELECT record_json FROM surface_inbox WHERE surface=$1 ORDER BY record_key",
                &[&normalize_surface_id(surface)],
            )
            .map_err(stringify)?,
        )
    }
    fn list_outbox(&self, surface: &str) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        rows_json(
            c.query(
                "SELECT record_json FROM surface_outbox WHERE surface=$1 ORDER BY record_key",
                &[&normalize_surface_id(surface)],
            )
            .map_err(stringify)?,
        )
    }
    fn list_all_inbox(&self) -> Result<Vec<SurfaceInboxRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        rows_json(
            c.query(
                "SELECT record_json FROM surface_inbox ORDER BY record_key",
                &[],
            )
            .map_err(stringify)?,
        )
    }
    fn list_all_outbox(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        rows_json(
            c.query(
                "SELECT record_json FROM surface_outbox ORDER BY record_key",
                &[],
            )
            .map_err(stringify)?,
        )
    }
    fn list_trigger_events(&self, surface: &str) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        rows_json(c.query("SELECT record_json FROM surface_trigger_event WHERE surface=$1 ORDER BY record_key", &[&normalize_surface_id(surface)]).map_err(stringify)?)
    }
    fn list_delivery_events(&self, surface: &str) -> Result<Vec<SurfaceDeliveryEvent>, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        rows_json(c.query("SELECT record_json FROM surface_delivery_event WHERE surface=$1 ORDER BY created_at_ms,record_key", &[&normalize_surface_id(surface)]).map_err(stringify)?)
    }
    fn snapshot(&self, surface: &str) -> Result<SurfaceMessageSnapshot, String> {
        let surface = normalize_surface_id(surface);
        let inbox = self.list_inbox(&surface)?;
        let outbox = self.list_outbox(&surface)?;
        let trigger_events = self.list_trigger_events(&surface)?;
        let archived_outbox = outbox
            .iter()
            .filter(|r| r.status == "archived")
            .cloned()
            .collect::<Vec<_>>();
        Ok(SurfaceMessageSnapshot {
            kind: "surface.message_snapshot",
            surface: surface.clone(),
            message_root: self.diagnostic_root(),
            active_inbox: inbox
                .iter()
                .filter(|r| is_active_inbox(&r.status))
                .cloned()
                .collect(),
            terminal_inbox: inbox
                .iter()
                .filter(|r| !is_active_inbox(&r.status))
                .cloned()
                .collect(),
            active_trigger_events: trigger_events
                .iter()
                .filter(|r| is_active_trigger(&r.status))
                .cloned()
                .collect(),
            failed_trigger_events: trigger_events
                .iter()
                .filter(|r| r.status == "dead_letter")
                .cloned()
                .collect(),
            active_outbox: outbox
                .iter()
                .filter(|r| is_active_outbox(&r.status))
                .cloned()
                .collect(),
            terminal_outbox: outbox
                .iter()
                .filter(|r| is_terminal_outbox(&r.status))
                .cloned()
                .collect(),
            dead_letters: outbox
                .iter()
                .filter(|r| r.status == "dead_letter")
                .cloned()
                .collect(),
            archived_count: archived_outbox.len(),
            deliveries: self.list_delivery_events(&surface)?,
            inbox,
            outbox,
            trigger_events,
            archived_outbox,
        })
    }
    fn export_migration_snapshot(&self) -> Result<SurfaceMessageLedgerMigrationSnapshot, String> {
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        let ingress = load_ingress(&mut c)?;
        let snapshot = SurfaceMessageLedgerMigrationSnapshot {
            inbox: rows_json(
                c.query(
                    "SELECT record_json FROM surface_inbox ORDER BY record_key",
                    &[],
                )
                .map_err(stringify)?,
            )?,
            outbox: rows_json(
                c.query(
                    "SELECT record_json FROM surface_outbox ORDER BY record_key",
                    &[],
                )
                .map_err(stringify)?,
            )?,
            trigger_events: rows_json(
                c.query(
                    "SELECT record_json FROM surface_trigger_event ORDER BY record_key",
                    &[],
                )
                .map_err(stringify)?,
            )?,
            delivery_events: rows_json(
                c.query(
                    "SELECT record_json FROM surface_delivery_event ORDER BY record_key",
                    &[],
                )
                .map_err(stringify)?,
            )?,
            ingress_frames: ingress,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
    fn import_migration_snapshot(
        &self,
        snapshot: &SurfaceMessageLedgerMigrationSnapshot,
    ) -> Result<(), String> {
        snapshot.validate()?;
        let mut c = self.executor.checkout_runtime().map_err(stringify)?;
        let mut tx = c.transaction().map_err(stringify)?;
        for table in [
            "surface_inbox",
            "surface_outbox",
            "surface_trigger_event",
            "surface_delivery_event",
            "surface_ingress_frame",
        ] {
            let count: i64 = tx
                .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
                .map_err(stringify)?
                .try_get(0)
                .map_err(stringify)?;
            if count != 0 {
                return Err(format!(
                    "surface migration target table `{table}` is not empty"
                ));
            }
        }
        for r in &snapshot.inbox {
            store_inbox(&mut tx, r)?
        }
        for r in &snapshot.outbox {
            insert_outbox(&mut tx, r)?
        }
        for r in &snapshot.trigger_events {
            store_trigger(&mut tx, r)?
        }
        for r in &snapshot.delivery_events {
            insert_event(&mut tx, r)?
        }
        for r in &snapshot.ingress_frames {
            insert_ingress(&mut tx, r)?
        }
        tx.commit().map_err(stringify)
    }
}

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn as_i64(value: u32) -> i64 {
    i64::from(value)
}
fn limit_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
fn as_u32(value: i64, field: &str) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("surface `{field}` outside u32 range"))
}
fn hash_str(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn hash_json(value: &Value) -> String {
    hash_str(&serde_json::to_string(value).unwrap_or_default())
}
fn summarize_text(value: &str, limit: usize) -> String {
    let mut out = value.chars().take(limit).collect::<String>();
    if value.chars().count() > limit {
        out.push('…')
    }
    out
}
fn summarize_json(value: &Value, limit: usize) -> String {
    summarize_text(&serde_json::to_string(value).unwrap_or_default(), limit)
}
fn next_retry_at_ms(attempts: u32) -> i64 {
    now_ms().saturating_add(i64::from(attempts.saturating_add(1)).saturating_mul(1_000))
}
fn inbound_key(surface: &str, id: &str) -> String {
    format!("{surface}:{id}")
}
fn outbound_key(surface: &str, reply: Option<&str>, recipient: &str, text: &str) -> String {
    format!(
        "{surface}:{}:{recipient}:{}",
        reply.unwrap_or("manual"),
        hash_str(text)
    )
}
fn new_event_id() -> String {
    format!("surface-delivery-event-{}", uuid::Uuid::new_v4())
}
fn row_json<T: serde::de::DeserializeOwned>(row: &Row, index: usize) -> Result<T, String> {
    serde_json::from_value(row.try_get::<_, Value>(index).map_err(stringify)?).map_err(stringify)
}
fn rows_json<T: serde::de::DeserializeOwned>(rows: Vec<Row>) -> Result<Vec<T>, String> {
    rows.iter().map(|r| row_json(r, 0)).collect()
}
fn payload_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}
fn surface_session_id(surface: &str, payload: &Value) -> String {
    payload_string(payload, "session")
        .or_else(|| payload_string(payload, "session_id"))
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            let metadata = payload.get("metadata").unwrap_or(&Value::Null);
            let chat = payload_string(metadata, "chat_id")
                .or_else(|| payload_string(payload, "thread_id"))
                .unwrap_or_else(|| "default".into());
            let user = payload_string(payload, "user_id").unwrap_or_else(|| "unknown".into());
            format!("{surface}:{user}:{chat}")
        })
}
fn is_active_inbox(s: &str) -> bool {
    matches!(
        s,
        "received"
            | "processing"
            | "processed"
            | "replying"
            | "failure_notifying"
            | "reply_retry_scheduled"
    )
}
fn is_active_outbox(s: &str) -> bool {
    matches!(s, "queued" | "sending" | "retry_scheduled")
}
fn is_terminal_outbox(s: &str) -> bool {
    matches!(s, "sent" | "dead_letter" | "cancelled" | "archived")
}
fn is_active_trigger(s: &str) -> bool {
    matches!(s, "received" | "dispatching" | "retry_scheduled")
}
fn outbox_failure_notice(r: &SurfaceOutboxRecord) -> bool {
    r.request_json
        .get("metadata")
        .and_then(|m| m.get("failure_notice"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}
fn outbox_failure_reason(r: &SurfaceOutboxRecord) -> Option<String> {
    r.request_json
        .get("metadata")
        .and_then(|m| m.get("failure_reason"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| r.last_error.clone())
}

fn inbox_for_update<C: PostgresClient>(
    db: &mut C,
    key: &str,
) -> Result<SurfaceInboxRecord, String> {
    db.query_opt(
        "SELECT record_json FROM surface_inbox WHERE record_key=$1 FOR UPDATE",
        &[&key],
    )
    .map_err(stringify)?
    .map(|r| row_json(&r, 0))
    .transpose()?
    .ok_or_else(|| format!("surface inbox `{key}` not found"))
}
fn trigger_for_update<C: PostgresClient>(
    db: &mut C,
    key: &str,
) -> Result<SurfaceTriggerEventRecord, String> {
    db.query_opt(
        "SELECT record_json FROM surface_trigger_event WHERE record_key=$1 FOR UPDATE",
        &[&key],
    )
    .map_err(stringify)?
    .map(|r| row_json(&r, 0))
    .transpose()?
    .ok_or_else(|| format!("surface trigger event `{key}` not found"))
}
fn outbox_for_update<C: PostgresClient>(
    db: &mut C,
    id: &str,
) -> Result<SurfaceOutboxRecord, String> {
    db.query_opt(
        "SELECT record_json FROM surface_outbox WHERE delivery_id=$1 FOR UPDATE",
        &[&id],
    )
    .map_err(stringify)?
    .map(|r| row_json(&r, 0))
    .transpose()?
    .ok_or_else(|| format!("surface delivery `{id}` not found"))
}
fn outbox_by_key<C: PostgresClient>(db: &mut C, key: &str) -> Result<SurfaceOutboxRecord, String> {
    db.query_opt(
        "SELECT record_json FROM surface_outbox WHERE record_key=$1",
        &[&key],
    )
    .map_err(stringify)?
    .map(|r| row_json(&r, 0))
    .transpose()?
    .ok_or_else(|| format!("surface outbox `{key}` disappeared"))
}
fn get_outbox<C: PostgresClient>(
    db: &mut C,
    id: &str,
) -> Result<Option<SurfaceOutboxRecord>, String> {
    db.query_opt(
        "SELECT record_json FROM surface_outbox WHERE delivery_id=$1",
        &[&id],
    )
    .map_err(stringify)?
    .map(|r| row_json(&r, 0))
    .transpose()
}
fn store_inbox<C: PostgresClient>(db: &mut C, r: &SurfaceInboxRecord) -> Result<(), String> {
    let json = serde_json::to_value(r).map_err(stringify)?;
    db.execute("INSERT INTO surface_inbox(record_key,surface,status,next_retry_at_ms,updated_at_ms,record_json) VALUES($1,$2,$3,NULL,$4,$5) ON CONFLICT(record_key) DO UPDATE SET surface=EXCLUDED.surface,status=EXCLUDED.status,updated_at_ms=EXCLUDED.updated_at_ms,record_json=EXCLUDED.record_json", &[&r.idempotency_key,&r.surface,&r.status,&r.updated_at_ms,&json]).map_err(stringify)?;
    Ok(())
}
fn mark_inbox_by_message_tx<C: PostgresClient>(
    db: &mut C,
    surface: &str,
    message_id: &str,
    status: &str,
    error: Option<String>,
) -> Result<(), String> {
    let row = db
        .query_opt(
            "SELECT record_json FROM surface_inbox WHERE surface=$1 AND record_json->>'message_id'=$2 FOR UPDATE",
            &[&normalize_surface_id(surface), &message_id],
        )
        .map_err(stringify)?;
    let Some(row) = row else {
        return Ok(());
    };
    let mut record: SurfaceInboxRecord = row_json(&row, 0)?;
    if record.status != status || record.last_error != error {
        record.status = status.to_string();
        record.last_error = error;
        record.updated_at_ms = now_ms();
        store_inbox(db, &record)?;
        insert_event(
            db,
            &inbox_event(
                &record,
                status,
                serde_json::json!({
                    "runtime_turn_id": record.runtime_turn_id,
                    "last_error": record.last_error,
                }),
            ),
        )?;
    }
    Ok(())
}
fn store_trigger<C: PostgresClient>(
    db: &mut C,
    r: &SurfaceTriggerEventRecord,
) -> Result<(), String> {
    let json = serde_json::to_value(r).map_err(stringify)?;
    db.execute("INSERT INTO surface_trigger_event(record_key,surface,status,next_retry_at_ms,updated_at_ms,record_json) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(record_key) DO UPDATE SET surface=EXCLUDED.surface,status=EXCLUDED.status,next_retry_at_ms=EXCLUDED.next_retry_at_ms,updated_at_ms=EXCLUDED.updated_at_ms,record_json=EXCLUDED.record_json", &[&r.idempotency_key,&r.surface,&r.status,&r.next_retry_at_ms,&r.updated_at_ms,&json]).map_err(stringify)?;
    Ok(())
}
fn insert_outbox_if_absent<C: PostgresClient>(
    db: &mut C,
    r: &SurfaceOutboxRecord,
) -> Result<bool, String> {
    let json = serde_json::to_value(r).map_err(stringify)?;
    Ok(db.execute("INSERT INTO surface_outbox(record_key,delivery_id,surface,status,next_retry_at_ms,lease_until_ms,updated_at_ms,record_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(record_key) DO NOTHING", &[&r.idempotency_key,&r.delivery_id,&r.surface,&r.status,&r.next_retry_at_ms,&r.lease_until_ms,&r.updated_at_ms,&json]).map_err(stringify)?==1)
}
fn insert_outbox<C: PostgresClient>(db: &mut C, r: &SurfaceOutboxRecord) -> Result<(), String> {
    let inserted = insert_outbox_if_absent(db, r)?;
    if !inserted {
        return Err(format!(
            "duplicate surface outbox `{}` during migration",
            r.idempotency_key
        ));
    }
    Ok(())
}
fn store_outbox<C: PostgresClient>(db: &mut C, r: &SurfaceOutboxRecord) -> Result<(), String> {
    let json = serde_json::to_value(r).map_err(stringify)?;
    db.execute("UPDATE surface_outbox SET surface=$1,status=$2,next_retry_at_ms=$3,lease_until_ms=$4,updated_at_ms=$5,record_json=$6 WHERE delivery_id=$7", &[&r.surface,&r.status,&r.next_retry_at_ms,&r.lease_until_ms,&r.updated_at_ms,&json,&r.delivery_id]).map_err(stringify)?;
    Ok(())
}
fn insert_event<C: PostgresClient>(db: &mut C, e: &SurfaceDeliveryEvent) -> Result<(), String> {
    let json = serde_json::to_value(e).map_err(stringify)?;
    db.execute("INSERT INTO surface_delivery_event(record_key,surface,status,created_at_ms,record_json) VALUES($1,$2,$3,$4,$5) ON CONFLICT(record_key) DO NOTHING", &[&e.event_id,&e.surface,&e.status,&e.created_at_ms,&json]).map_err(stringify)?;
    Ok(())
}
fn inbox_event(r: &SurfaceInboxRecord, kind: &str, detail: Value) -> SurfaceDeliveryEvent {
    SurfaceDeliveryEvent {
        event_id: new_event_id(),
        surface: r.surface.clone(),
        delivery_id: None,
        message_id: Some(r.message_id.clone()),
        kind: format!("inbox.{kind}"),
        status: r.status.clone(),
        detail_json: detail,
        created_at_ms: now_ms(),
    }
}
fn trigger_event(r: &SurfaceTriggerEventRecord) -> SurfaceDeliveryEvent {
    SurfaceDeliveryEvent {
        event_id: new_event_id(),
        surface: r.surface.clone(),
        delivery_id: None,
        message_id: None,
        kind: format!("trigger_event.{}", r.status),
        status: r.status.clone(),
        detail_json: serde_json::json!({"event_id":r.trigger.event_id,"event_type":r.event_type,"idempotency_key":r.idempotency_key,"attempts":r.attempts,"max_attempts":r.max_attempts,"next_retry_at_ms":r.next_retry_at_ms,"last_error":r.last_error}),
        created_at_ms: now_ms(),
    }
}
fn outbox_event(r: &SurfaceOutboxRecord, kind: &str, detail: Value) -> SurfaceDeliveryEvent {
    SurfaceDeliveryEvent {
        event_id: new_event_id(),
        surface: r.surface.clone(),
        delivery_id: Some(r.delivery_id.clone()),
        message_id: r.reply_to_message_id.clone(),
        kind: format!("outbox.{kind}"),
        status: r.status.clone(),
        detail_json: detail,
        created_at_ms: now_ms(),
    }
}
fn load_ingress<C: PostgresClient>(db: &mut C) -> Result<Vec<SurfaceIngressFrameRecord>, String> {
    db.query("SELECT record_key,surface,session_id,status,attempts,max_attempts,next_retry_at_ms,claim_owner,lease_until_ms,created_at_ms,updated_at_ms,frame_json,last_error FROM surface_ingress_frame ORDER BY record_key",&[]).map_err(stringify)?.iter().map(|r|Ok(SurfaceIngressFrameRecord{record_key:r.try_get(0).map_err(stringify)?,surface:r.try_get(1).map_err(stringify)?,session_id:r.try_get(2).map_err(stringify)?,status:r.try_get(3).map_err(stringify)?,attempts:as_u32(r.try_get(4).map_err(stringify)?,"attempts")?,max_attempts:as_u32(r.try_get(5).map_err(stringify)?,"max_attempts")?,next_retry_at_ms:r.try_get(6).map_err(stringify)?,claim_owner:r.try_get(7).map_err(stringify)?,lease_until_ms:r.try_get(8).map_err(stringify)?,created_at_ms:r.try_get(9).map_err(stringify)?,updated_at_ms:r.try_get(10).map_err(stringify)?,frame:row_json(r,11)?,last_error:r.try_get(12).map_err(stringify)?})).collect()
}
fn insert_ingress<C: PostgresClient>(
    db: &mut C,
    r: &SurfaceIngressFrameRecord,
) -> Result<(), String> {
    let frame = serde_json::to_value(&r.frame).map_err(stringify)?;
    db.execute("INSERT INTO surface_ingress_frame(record_key,surface,session_id,status,attempts,max_attempts,next_retry_at_ms,claim_owner,lease_until_ms,created_at_ms,updated_at_ms,frame_json,last_error) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)", &[&r.record_key,&r.surface,&r.session_id,&r.status,&as_i64(r.attempts),&as_i64(r.max_attempts),&r.next_retry_at_ms,&r.claim_owner,&r.lease_until_ms,&r.created_at_ms,&r.updated_at_ms,&frame,&r.last_error]).map_err(stringify)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SurfaceMessageMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub inbox_count: usize,
    pub outbox_count: usize,
    pub trigger_event_count: usize,
    pub delivery_event_count: usize,
    pub ingress_frame_count: usize,
}
pub fn copy_quiesced_surface_message_ledger(
    source: &dyn SurfaceMessageLedger,
    target: &dyn SurfaceMessageLedger,
    manifest_path: impl AsRef<Path>,
) -> Result<SurfaceMessageMigrationManifest, String> {
    let snapshot = source.export_migration_snapshot()?;
    snapshot.validate()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    let target_digest = target.export_migration_snapshot()?.canonical_digest()?;
    if source_digest != target_digest {
        return Err("surface message migration digest mismatch".into());
    }
    let manifest = SurfaceMessageMigrationManifest {
        domain: DOMAIN.into(),
        source_digest,
        target_digest,
        inbox_count: snapshot.inbox.len(),
        outbox_count: snapshot.outbox.len(),
        trigger_event_count: snapshot.trigger_events.len(),
        delivery_event_count: snapshot.delivery_events.len(),
        ingress_frame_count: snapshot.ingress_frames.len(),
    };
    if let Some(parent) = manifest_path.as_ref().parent() {
        fs::create_dir_all(parent).map_err(stringify)?
    }
    let tmp = PathBuf::from(format!(
        "{}.{}.tmp",
        manifest_path.as_ref().display(),
        uuid::Uuid::new_v4()
    ));
    fs::write(
        &tmp,
        serde_json::to_vec_pretty(&manifest).map_err(stringify)?,
    )
    .map_err(stringify)?;
    fs::rename(tmp, manifest_path).map_err(stringify)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use harness_contract::managed_agent::ManagedAgentTriggerEvent;
    use storage::StaticSecretRefResolver;

    use super::*;

    fn ledger_from_url(url: String, identity: &str) -> Arc<PostgresSurfaceMessageLedger> {
        let resolver = StaticSecretRefResolver::new([("v572-test".to_string(), url)]);
        Arc::new(
            PostgresSurfaceMessageLedger::connect(
                PostgresConnectionConfig::new(identity, "v572-test", identity),
                &resolver,
            )
            .expect("connect isolated PostgreSQL test database"),
        )
    }

    fn clear(ledger: &PostgresSurfaceMessageLedger) {
        let mut connection = ledger
            .executor()
            .checkout_runtime()
            .expect("checkout PostgreSQL");
        connection
            .batch_execute(
                "TRUNCATE TABLE surface_delivery_event, surface_ingress_frame, surface_outbox, \
                 surface_trigger_event, surface_inbox",
            )
            .expect("clear isolated Surface tables");
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

    fn send_request(key: &str) -> SurfaceSendRequest {
        SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "chat-1".to_string(),
            thread: Some("thread-1".to_string()),
            text: "durable PostgreSQL delivery".to_string(),
            idempotency_key: Some(key.to_string()),
            metadata: serde_json::json!({"test": "v572"}),
        }
    }

    fn correlation() -> SurfaceTurnCorrelation {
        SurfaceTurnCorrelation {
            surface: "feishu".to_string(),
            message_id: "message-1".to_string(),
            inbox_idempotency_key: "feishu:message-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            execution_id: "execution-1".to_string(),
            reply_to_message_id: "message-1".to_string(),
            reply_idempotency_key: "reply-1".to_string(),
            terminal_id: None,
            terminal_delivery_revision: 0,
        }
    }

    /// The only live-database adapter test.  It is opt-in so ordinary unit
    /// test runs never use ambient connection credentials.
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_postgres_preserves_contract_and_serializes_competing_delivery_claims() {
        let ledger = ledger_from_url(
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required"),
            "surface-v572-real",
        );
        clear(&ledger);
        let checksum_original = PostgresMigrationSpec {
            id: "surface_message.test_checksum",
            domain: "surface_message_test_checksum",
            version: 1,
            description: "create Surface migration checksum probe",
            statements: &["CREATE TABLE surface_message_checksum_probe(id TEXT PRIMARY KEY)"],
        };
        ledger
            .executor()
            .apply_migrations(checksum_original.domain, &[checksum_original.clone()])
            .unwrap();
        let checksum_changed = PostgresMigrationSpec {
            statements: &["CREATE TABLE surface_message_checksum_probe(id TEXT PRIMARY KEY, state TEXT NOT NULL)"],
            ..checksum_original
        };
        assert!(ledger
            .executor()
            .apply_migrations(checksum_changed.domain, &[checksum_changed])
            .expect_err("changed migration must fail closed")
            .to_string()
            .contains("checksum mismatch"));
        let contract: Arc<dyn SurfaceMessageLedger> = ledger.clone();

        let ingress = SurfaceFrame::Event {
            surface: "feishu".to_string(),
            event: "message.received".to_string(),
            payload: serde_json::json!({"session_id":"session-1", "message_id":"message-1"}),
        };
        let ingress_key = contract.persist_ingress_frame(&ingress).unwrap();
        assert_eq!(
            contract.persist_ingress_frame(&ingress).unwrap(),
            ingress_key
        );
        let claims = contract.claim_ingress_frames("worker-1", 8, 5_000).unwrap();
        assert_eq!(claims.len(), 1);
        contract
            .complete_ingress_frame(&claims[0].record_key)
            .unwrap();
        assert!(contract
            .claim_ingress_frames("worker-2", 8, 5_000)
            .unwrap()
            .is_empty());

        let received = contract
            .record_inbox_received(
                "feishu",
                "message-1",
                &serde_json::json!({"text":"hello"}),
                "session-1",
                Some("thread-1".to_string()),
                Some("user-1".to_string()),
            )
            .unwrap();
        assert!(!received.duplicate);
        assert!(
            contract
                .record_inbox_received(
                    "feishu",
                    "message-1",
                    &serde_json::json!({"text":"hello"}),
                    "session-1",
                    None,
                    None,
                )
                .unwrap()
                .duplicate
        );
        let inbox_key = received.record.idempotency_key;
        contract.mark_inbox_processing(&inbox_key).unwrap();
        contract
            .mark_inbox_processed(&inbox_key, Some("turn-1".to_string()))
            .unwrap();
        contract
            .mark_inbox_admitted(&inbox_key, correlation())
            .unwrap();
        contract
            .record_inbox_terminal_delivery(&inbox_key, "terminal-1")
            .unwrap();

        let trigger = trigger_event("contract");
        assert!(
            !contract
                .record_trigger_event_received(
                    "feishu",
                    "message.received",
                    &trigger,
                    &serde_json::json!({"message_id":"message-1"}),
                )
                .unwrap()
                .duplicate
        );
        assert!(contract
            .mark_trigger_event_dispatching(&trigger.idempotency_key)
            .unwrap()
            .is_some());
        assert_eq!(
            contract
                .mark_trigger_event_accepted(&trigger.idempotency_key)
                .unwrap()
                .status,
            "accepted"
        );

        let request = send_request("concurrent-delivery");
        let mut workers = Vec::new();
        for _ in 0..16 {
            let ledger = ledger.clone();
            let request = request.clone();
            workers.push(std::thread::spawn(move || {
                ledger
                    .queue_outbox(
                        &request,
                        Some("session-1".to_string()),
                        Some("message-1".to_string()),
                    )
                    .unwrap()
            }));
        }
        let deliveries = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert!(deliveries
            .iter()
            .all(|record| record.delivery_id == deliveries[0].delivery_id));
        assert_eq!(contract.list_outbox("feishu").unwrap().len(), 1);

        let id = deliveries[0].delivery_id.clone();
        let mut claim_workers = Vec::new();
        for _ in 0..16 {
            let ledger = ledger.clone();
            let id = id.clone();
            claim_workers.push(std::thread::spawn(move || {
                ledger.mark_delivery_sending(&id).is_ok()
            }));
        }
        assert_eq!(
            claim_workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .filter(|claimed| *claimed)
                .count(),
            1
        );
        let sent = contract
            .mark_delivery_sent(
                &id,
                &SurfaceOperationResult::ok(
                    "feishu",
                    serde_json::json!({"message_id":"provider-1"}),
                ),
            )
            .unwrap();
        assert_eq!(sent.status, "sent");
        assert_eq!(
            contract
                .get_inbox_message("feishu", "message-1")
                .unwrap()
                .unwrap()
                .status,
            "replied"
        );
        assert!(contract
            .list_delivery_events("feishu")
            .unwrap()
            .iter()
            .any(|event| event.kind == "outbox.sent" && event.status == "sent"));
        let snapshot = contract.export_migration_snapshot().unwrap();
        assert_eq!(
            snapshot.canonical_digest().unwrap(),
            contract
                .export_migration_snapshot()
                .unwrap()
                .canonical_digest()
                .unwrap()
        );
    }

    #[test]
    #[ignore = "requires isolated COWD_TEST_POSTGRES_URL and COWD_TEST_POSTGRES_TARGET_URL"]
    fn real_postgres_to_postgres_quiesced_copy_is_digest_exact_and_target_only() {
        let source = ledger_from_url(
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required"),
            "surface-v572-copy-source",
        );
        let target = ledger_from_url(
            std::env::var("COWD_TEST_POSTGRES_TARGET_URL")
                .expect("COWD_TEST_POSTGRES_TARGET_URL is required"),
            "surface-v572-copy-target",
        );
        clear(&source);
        clear(&target);
        source
            .persist_ingress_frame(&SurfaceFrame::Event {
                surface: "feishu".to_string(),
                event: "message.received".to_string(),
                payload: serde_json::json!({"session_id":"copy-session", "message_id":"copy-message"}),
            })
            .unwrap();
        source
            .record_inbox_received(
                "feishu",
                "copy-message",
                &serde_json::json!({"text":"copy"}),
                "copy-session",
                None,
                None,
            )
            .unwrap();
        source
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &trigger_event("copy"),
                &serde_json::json!({"copy":true}),
            )
            .unwrap();
        source
            .queue_outbox(
                &send_request("copy-delivery"),
                Some("copy-session".to_string()),
                None,
            )
            .unwrap();

        let directory = tempfile::tempdir().unwrap();
        let manifest = copy_quiesced_surface_message_ledger(
            &*source,
            &*target,
            directory
                .path()
                .join("surface-message-migration-manifest.json"),
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
    }
}
