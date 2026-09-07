//! Explicit process-local Surface ledger for isolated host tests.

use std::{collections::BTreeMap, path::PathBuf, sync::Mutex};

use sha2::{Digest, Sha256};
use surface::*;

const MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, Default)]
struct State {
    ingress: BTreeMap<String, (SurfaceFrame, String, u32)>,
    inbox: BTreeMap<String, SurfaceInboxRecord>,
    triggers: BTreeMap<String, SurfaceTriggerEventRecord>,
    outbox: BTreeMap<String, SurfaceOutboxRecord>,
    events: Vec<SurfaceDeliveryEvent>,
    archived: Vec<SurfaceOutboxRecord>,
}

#[derive(Debug, Default)]
pub(crate) struct EphemeralSurfaceMessageLedger {
    state: Mutex<State>,
}

impl EphemeralSurfaceMessageLedger {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    #[cfg(test)]
    pub(crate) fn ingress_frame_count(&self) -> usize {
        self.state.lock().unwrap().ingress.len()
    }
    fn update_inbox(
        &self,
        key: &str,
        change: impl FnOnce(&mut SurfaceInboxRecord) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        let r = s
            .inbox
            .get_mut(key)
            .ok_or_else(|| format!("surface inbox `{key}` not found"))?;
        change(r)?;
        r.updated_at_ms = now();
        Ok(())
    }
    fn update_trigger(
        &self,
        key: &str,
        change: impl FnOnce(&mut SurfaceTriggerEventRecord) -> Result<(), String>,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let mut s = self.state.lock().unwrap();
        let r = s
            .triggers
            .get_mut(key)
            .ok_or_else(|| format!("surface trigger `{key}` not found"))?;
        change(r)?;
        r.updated_at_ms = now();
        Ok(r.clone())
    }
    fn update_outbox(
        &self,
        id: &str,
        change: impl FnOnce(&mut SurfaceOutboxRecord) -> Result<(), String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let mut s = self.state.lock().unwrap();
        let r = s
            .outbox
            .values_mut()
            .find(|r| r.delivery_id == id)
            .ok_or_else(|| format!("surface delivery `{id}` not found"))?;
        change(r)?;
        r.updated_at_ms = now();
        Ok(r.clone())
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn summary(value: &str) -> String {
    let mut v = value.chars().take(240).collect::<String>();
    if value.chars().count() > 240 {
        v.push('…')
    }
    v
}
fn event(
    surface: &str,
    delivery: Option<String>,
    message: Option<String>,
    kind: &str,
    status: &str,
) -> SurfaceDeliveryEvent {
    SurfaceDeliveryEvent {
        event_id: format!("surface-event-{}", uuid::Uuid::new_v4()),
        surface: surface.into(),
        delivery_id: delivery,
        message_id: message,
        kind: kind.into(),
        status: status.into(),
        detail_json: serde_json::json!({}),
        created_at_ms: now(),
    }
}

impl SurfaceMessageLedger for EphemeralSurfaceMessageLedger {
    fn diagnostic_root(&self) -> PathBuf {
        PathBuf::from("ephemeral://surface-ledger")
    }
    fn persist_ingress_frame(&self, frame: &SurfaceFrame) -> Result<String, String> {
        if !matches!(frame, SurfaceFrame::Event { .. }) {
            return Err("only Surface event frames can enter ingress".into());
        }
        let key = format!(
            "surface-ingress:{}",
            hash(&serde_json::to_string(frame).map_err(|e| e.to_string())?)
        );
        self.state
            .lock()
            .unwrap()
            .ingress
            .entry(key.clone())
            .or_insert((frame.clone(), "pending".into(), 0));
        Ok(key)
    }
    fn claim_ingress_frames(
        &self,
        owner: &str,
        limit: usize,
        _lease: i64,
    ) -> Result<Vec<SurfaceIngressClaim>, String> {
        let mut s = self.state.lock().unwrap();
        let mut out = Vec::new();
        for (key, (frame, status, attempts)) in s
            .ingress
            .iter_mut()
            .filter(|(_, (_, status, _))| status == "pending" || status == "retry_scheduled")
            .take(limit)
        {
            *status = format!("claimed:{owner}");
            *attempts += 1;
            out.push(SurfaceIngressClaim {
                record_key: key.clone(),
                frame: frame.clone(),
            })
        }
        Ok(out)
    }
    fn complete_ingress_frame(&self, key: &str) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        let r = s
            .ingress
            .get_mut(key)
            .ok_or_else(|| format!("surface ingress `{key}` not found"))?;
        r.1 = "completed".into();
        Ok(())
    }
    fn fail_ingress_frame(&self, key: &str, _error: &str) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        let r = s
            .ingress
            .get_mut(key)
            .ok_or_else(|| format!("surface ingress `{key}` not found"))?;
        r.1 = if r.2 >= MAX_ATTEMPTS {
            "dead_letter"
        } else {
            "retry_scheduled"
        }
        .into();
        Ok(())
    }
    fn record_inbox_received(
        &self,
        surface: &str,
        message: &str,
        payload: &serde_json::Value,
        session: &str,
        thread: Option<String>,
        sender: Option<String>,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<SurfaceInboxReceipt, String> {
        let key = format!("{surface}:{message}");
        let mut s = self.state.lock().unwrap();
        if let Some(r) = s.inbox.get_mut(&key) {
            r.stage_session_projections(projections)?;
            return Ok(SurfaceInboxReceipt {
                record: r.clone(),
                duplicate: true,
            });
        }
        let at = now();
        let raw = serde_json::to_string(payload).unwrap_or_default();
        let mut r = SurfaceInboxRecord {
            id: key.clone(),
            surface: surface.into(),
            message_id: message.into(),
            idempotency_key: key.clone(),
            thread_id: thread,
            sender_id: sender,
            payload_hash: hash(&raw),
            payload_summary: summary(&raw),
            payload_json: payload.clone(),
            status: "received".into(),
            received_at_ms: at,
            updated_at_ms: at,
            runtime_session_id: Some(session.into()),
            runtime_turn_id: None,
            correlation: None,
            session_projections: Vec::new(),
            last_error: None,
        };
        r.stage_session_projections(projections)?;
        s.events.push(event(
            surface,
            None,
            Some(message.into()),
            "inbox",
            "received",
        ));
        s.inbox.insert(key, r.clone());
        Ok(SurfaceInboxReceipt {
            record: r,
            duplicate: false,
        })
    }
    fn mark_inbox_processing(
        &self,
        key: &str,
        p: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.update_inbox(key, |r| {
            r.stage_session_projections(p)?;
            r.status = "processing".into();
            r.last_error = None;
            Ok(())
        })
    }
    fn mark_inbox_processed(&self, key: &str, turn: Option<String>) -> Result<(), String> {
        self.update_inbox(key, |r| {
            r.status = "processed".into();
            if turn.is_some() {
                r.runtime_turn_id = turn
            }
            Ok(())
        })
    }
    fn mark_inbox_admitted(
        &self,
        key: &str,
        c: SurfaceTurnCorrelation,
        p: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.update_inbox(key, |r| {
            r.stage_session_projections(p)?;
            r.status = "admitted".into();
            r.correlation = Some(c);
            Ok(())
        })
    }
    fn record_inbox_terminal_delivery(&self, key: &str, id: &str) -> Result<(), String> {
        self.update_inbox(key, |r| {
            let c = r
                .correlation
                .as_mut()
                .ok_or_else(|| "inbox correlation missing".to_string())?;
            c.terminal_id = Some(id.into());
            c.terminal_delivery_revision += 1;
            Ok(())
        })
    }
    fn mark_inbox_replied(
        &self,
        key: &str,
        p: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.update_inbox(key, |r| {
            r.stage_session_projections(p)?;
            r.status = "replied".into();
            Ok(())
        })
    }
    fn stage_inbox_projections(
        &self,
        key: &str,
        p: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        self.update_inbox(key, |r| r.stage_session_projections(p))
    }
    fn mark_inbox_projection_applied(&self, key: &str, id: &str, at: i64) -> Result<(), String> {
        self.update_inbox(key, |r| r.mark_session_projection_applied(id, at))
    }
    fn mark_inbox_projection_failed(&self, key: &str, id: &str, e: &str) -> Result<(), String> {
        self.update_inbox(key, |r| r.mark_session_projection_failed(id, e))
    }
    fn mark_inbox_reply_failed(&self, key: &str, e: &str) -> Result<(), String> {
        self.update_inbox(key, |r| {
            r.status = "reply_failed".into();
            r.last_error = Some(e.into());
            Ok(())
        })
    }
    fn mark_inbox_failed(&self, key: &str, e: &str) -> Result<(), String> {
        self.update_inbox(key, |r| {
            r.status = "failed".into();
            r.last_error = Some(e.into());
            Ok(())
        })
    }
    fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &harness_contract::managed_agent::ManagedAgentTriggerEvent,
        payload: &serde_json::Value,
    ) -> Result<SurfaceTriggerEventReceipt, String> {
        let mut s = self.state.lock().unwrap();
        if let Some(r) = s.triggers.get(&trigger.idempotency_key) {
            return Ok(SurfaceTriggerEventReceipt {
                record: r.clone(),
                duplicate: true,
            });
        }
        let at = now();
        let r = SurfaceTriggerEventRecord {
            idempotency_key: trigger.idempotency_key.clone(),
            surface: surface.into(),
            event_type: event_type.into(),
            trigger: trigger.clone(),
            payload_json: payload.clone(),
            status: "received".into(),
            attempts: 0,
            max_attempts: MAX_ATTEMPTS,
            next_retry_at_ms: Some(at),
            created_at_ms: at,
            updated_at_ms: at,
            accepted_at_ms: None,
            last_error: None,
        };
        s.triggers.insert(r.idempotency_key.clone(), r.clone());
        Ok(SurfaceTriggerEventReceipt {
            record: r,
            duplicate: false,
        })
    }
    fn mark_trigger_event_dispatching(
        &self,
        key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String> {
        let mut s = self.state.lock().unwrap();
        let Some(r) = s.triggers.get_mut(key) else {
            return Ok(None);
        };
        if !matches!(r.status.as_str(), "received" | "retry_scheduled") {
            return Ok(None);
        }
        r.status = "dispatching".into();
        r.attempts += 1;
        Ok(Some(r.clone()))
    }
    fn mark_trigger_event_accepted(&self, key: &str) -> Result<SurfaceTriggerEventRecord, String> {
        self.update_trigger(key, |r| {
            r.status = "accepted".into();
            r.accepted_at_ms = Some(now());
            r.last_error = None;
            Ok(())
        })
    }
    fn mark_trigger_event_failed(
        &self,
        key: &str,
        e: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.update_trigger(key, |r| {
            r.status = if r.attempts >= r.max_attempts {
                "dead_letter"
            } else {
                "retry_scheduled"
            }
            .into();
            r.next_retry_at_ms = Some(now());
            r.last_error = Some(e.into());
            Ok(())
        })
    }
    fn retry_trigger_event(
        &self,
        surface: &str,
        key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.update_trigger(key, |r| {
            if r.surface != surface {
                return Err("surface mismatch".into());
            }
            r.status = "received".into();
            r.attempts = 0;
            r.last_error = None;
            Ok(())
        })
    }
    fn queue_outbox(
        &self,
        request: &SurfaceSendRequest,
        source: Option<String>,
        reply: Option<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let raw = serde_json::to_string(request).map_err(|e| e.to_string())?;
        let key = request.idempotency_key.clone().unwrap_or_else(|| {
            format!(
                "{}:{}:{}",
                request.surface,
                request.recipient,
                hash(&request.text)
            )
        });
        let mut s = self.state.lock().unwrap();
        if let Some(r) = s.outbox.get(&key) {
            return Ok(r.clone());
        }
        let at = now();
        let r = SurfaceOutboxRecord {
            delivery_id: format!("surface-delivery-{}", uuid::Uuid::new_v4()),
            surface: request.surface.clone(),
            recipient: request.recipient.clone(),
            thread_id: request.thread.clone(),
            idempotency_key: key.clone(),
            text_hash: hash(&request.text),
            text_summary: summary(&request.text),
            request_json: serde_json::from_str(&raw).unwrap_or_default(),
            status: "queued".into(),
            attempts: 0,
            max_attempts: MAX_ATTEMPTS,
            next_retry_at_ms: None,
            claim_owner: None,
            lease_until_ms: None,
            created_at_ms: at,
            updated_at_ms: at,
            sent_at_ms: None,
            last_error: None,
            source_session_id: source,
            reply_to_message_id: reply,
        };
        s.events.push(event(
            &r.surface,
            Some(r.delivery_id.clone()),
            None,
            "outbox",
            "queued",
        ));
        s.outbox.insert(key, r.clone());
        Ok(r)
    }
    fn mark_delivery_sending(&self, id: &str) -> Result<SurfaceOutboxRecord, String> {
        self.update_outbox(id, |r| {
            if !matches!(r.status.as_str(), "queued" | "retry_scheduled") {
                return Err(format!("delivery status is {}", r.status));
            }
            r.status = "sending".into();
            r.attempts += 1;
            Ok(())
        })
    }
    fn mark_delivery_sent(
        &self,
        id: &str,
        result: &SurfaceOperationResult,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.update_outbox(id, |r| {
            r.status = "sent".into();
            r.sent_at_ms = Some(now());
            r.last_error = None;
            if let Some(mid) = &result.message_id {
                r.request_json["provider_message_id"] = serde_json::Value::String(mid.clone())
            }
            Ok(())
        })
    }
    fn mark_delivery_failed(
        &self,
        id: &str,
        e: &str,
        retryable: bool,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.update_outbox(id, |r| {
            r.status = if retryable && r.attempts < r.max_attempts {
                "retry_scheduled"
            } else {
                "dead_letter"
            }
            .into();
            r.next_retry_at_ms = Some(now());
            r.last_error = Some(e.into());
            Ok(())
        })
    }
    fn mark_delivery_dead_letter(
        &self,
        id: &str,
        reason: &str,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.update_outbox(id, |r| {
            r.status = "dead_letter".into();
            r.last_error = Some(reason.into());
            Ok(())
        })
    }
    fn mark_delivery_replayed(&self, id: &str) -> Result<SurfaceOutboxRecord, String> {
        self.update_outbox(id, |r| {
            r.status = "queued".into();
            r.attempts = 0;
            r.last_error = None;
            Ok(())
        })
    }
    fn archive_dead_letters(
        &self,
        surface: &str,
        older: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let mut s = self.state.lock().unwrap();
        let keys = s
            .outbox
            .iter()
            .filter(|(_, r)| {
                r.surface == surface
                    && r.status == "dead_letter"
                    && older.is_none_or(|v| r.updated_at_ms <= v)
            })
            .take(limit)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        let mut out = Vec::new();
        for k in keys {
            if let Some(mut r) = s.outbox.remove(&k) {
                r.status = "archived".into();
                s.archived.push(r.clone());
                out.push(r)
            }
        }
        Ok(out)
    }
    fn purge_archived_events(
        &self,
        surface: &str,
        older: Option<i64>,
        limit: usize,
    ) -> Result<usize, String> {
        let mut s = self.state.lock().unwrap();
        let before = s.archived.len();
        let mut removed = 0;
        s.archived.retain(|r| {
            let purge = removed < limit
                && r.surface == surface
                && older.is_none_or(|v| r.updated_at_ms <= v);
            if purge {
                removed += 1
            }
            !purge
        });
        Ok(before - s.archived.len())
    }
    fn get_outbox_by_delivery(&self, id: &str) -> Result<Option<SurfaceOutboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .outbox
            .values()
            .find(|r| r.delivery_id == id)
            .cloned())
    }
    fn due_retry_deliveries(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .outbox
            .values()
            .filter(|r| matches!(r.status.as_str(), "queued" | "retry_scheduled"))
            .cloned()
            .collect())
    }
    fn due_trigger_event_retries(&self) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .triggers
            .values()
            .filter(|r| matches!(r.status.as_str(), "received" | "retry_scheduled"))
            .cloned()
            .collect())
    }
    fn get_inbox_by_key(&self, key: &str) -> Result<Option<SurfaceInboxRecord>, String> {
        Ok(self.state.lock().unwrap().inbox.get(key).cloned())
    }
    fn get_inbox_message(
        &self,
        surface: &str,
        message: &str,
    ) -> Result<Option<SurfaceInboxRecord>, String> {
        self.get_inbox_by_key(&format!("{surface}:{message}"))
    }
    fn list_inbox(&self, surface: &str) -> Result<Vec<SurfaceInboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .inbox
            .values()
            .filter(|r| r.surface == surface)
            .cloned()
            .collect())
    }
    fn list_outbox(&self, surface: &str) -> Result<Vec<SurfaceOutboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .outbox
            .values()
            .filter(|r| r.surface == surface)
            .cloned()
            .collect())
    }
    fn list_all_inbox(&self) -> Result<Vec<SurfaceInboxRecord>, String> {
        Ok(self.state.lock().unwrap().inbox.values().cloned().collect())
    }
    fn list_all_outbox(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .outbox
            .values()
            .cloned()
            .collect())
    }
    fn list_trigger_events(&self, surface: &str) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .triggers
            .values()
            .filter(|r| r.surface == surface)
            .cloned()
            .collect())
    }
    fn list_delivery_events(&self, surface: &str) -> Result<Vec<SurfaceDeliveryEvent>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|r| r.surface == surface)
            .cloned()
            .collect())
    }
    fn snapshot(&self, surface: &str) -> Result<SurfaceMessageSnapshot, String> {
        let s = self.state.lock().unwrap();
        let inbox = s
            .inbox
            .values()
            .filter(|r| r.surface == surface)
            .cloned()
            .collect::<Vec<_>>();
        let outbox = s
            .outbox
            .values()
            .filter(|r| r.surface == surface)
            .cloned()
            .collect::<Vec<_>>();
        let triggers = s
            .triggers
            .values()
            .filter(|r| r.surface == surface)
            .cloned()
            .collect::<Vec<_>>();
        Ok(SurfaceMessageSnapshot {
            kind: "surface.message.snapshot",
            surface: surface.into(),
            message_root: self.diagnostic_root(),
            active_inbox: inbox
                .iter()
                .filter(|r| !matches!(r.status.as_str(), "replied" | "failed"))
                .cloned()
                .collect(),
            terminal_inbox: inbox
                .iter()
                .filter(|r| matches!(r.status.as_str(), "replied" | "failed"))
                .cloned()
                .collect(),
            inbox,
            active_trigger_events: triggers
                .iter()
                .filter(|r| !matches!(r.status.as_str(), "accepted" | "dead_letter"))
                .cloned()
                .collect(),
            failed_trigger_events: triggers
                .iter()
                .filter(|r| r.status == "dead_letter")
                .cloned()
                .collect(),
            trigger_events: triggers,
            active_outbox: outbox
                .iter()
                .filter(|r| !matches!(r.status.as_str(), "sent" | "dead_letter"))
                .cloned()
                .collect(),
            terminal_outbox: outbox
                .iter()
                .filter(|r| matches!(r.status.as_str(), "sent" | "dead_letter"))
                .cloned()
                .collect(),
            outbox,
            deliveries: s
                .events
                .iter()
                .filter(|r| r.surface == surface)
                .cloned()
                .collect(),
            dead_letters: s
                .outbox
                .values()
                .filter(|r| r.surface == surface && r.status == "dead_letter")
                .cloned()
                .collect(),
            archived_outbox: s
                .archived
                .iter()
                .filter(|r| r.surface == surface)
                .cloned()
                .collect(),
            archived_count: s.archived.iter().filter(|r| r.surface == surface).count(),
        })
    }
}
