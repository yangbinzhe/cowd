//! Runtime-owned, non-blocking Skill usage receipt writer.
//!
//! Gateway may observe cache mechanics while implementing the instruction
//! source, but it never receives the EventStore and cannot mint an
//! authoritative usage event.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc, Arc,
};

use harness_contract::skill::{
    SkillUsageKind, SkillUsageReceipt, SKILL_USAGE_RECEIPT_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};

use super::{
    governance::SkillRevisionPointerCache, RuntimeSkillUsageContext, RuntimeSkillUsageSink,
    RuntimeSkillUsageSinkHealth, SkillInvocation,
};
use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};

pub const SKILL_USAGE_RECEIPT_EVENT_KIND: &str = "skill.usage.receipt.v1";
const SKILL_USAGE_QUEUE_CAPACITY: usize = 2_048;
const SKILL_USAGE_BATCH_LIMIT: usize = 128;

#[derive(Debug)]
pub struct RuntimeSkillUsageRecorder {
    store: Arc<RuntimeEventStore>,
    pointer_cache: Arc<SkillRevisionPointerCache>,
    sender: mpsc::SyncSender<SkillUsageReceipt>,
    accepted: Arc<AtomicU64>,
    persisted: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    persistence_failures: Arc<AtomicU64>,
}

impl RuntimeSkillUsageRecorder {
    #[must_use]
    pub fn new(store: Arc<RuntimeEventStore>) -> Self {
        Self::with_pointer_cache(store, Arc::new(SkillRevisionPointerCache::default()))
    }

    #[must_use]
    pub fn with_pointer_cache(
        store: Arc<RuntimeEventStore>,
        pointer_cache: Arc<SkillRevisionPointerCache>,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel(SKILL_USAGE_QUEUE_CAPACITY);
        let accepted = Arc::new(AtomicU64::new(0));
        let persisted = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let persistence_failures = Arc::new(AtomicU64::new(0));
        let worker_persisted = Arc::clone(&persisted);
        let worker_failures = Arc::clone(&persistence_failures);
        let worker_store = Arc::clone(&store);
        std::thread::Builder::new()
            .name("cowd-runtime-skill-usage".to_string())
            .spawn(move || {
                while let Ok(first) = receiver.recv() {
                    let mut batch = Vec::with_capacity(SKILL_USAGE_BATCH_LIMIT);
                    batch.push(first);
                    while batch.len() < SKILL_USAGE_BATCH_LIMIT {
                        match receiver.try_recv() {
                            Ok(receipt) => batch.push(receipt),
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                    for receipt in batch {
                        match persist_receipt(&worker_store, &receipt) {
                            Ok(()) => {
                                worker_persisted.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(error) => {
                                worker_failures.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(
                                    %error,
                                    receipt_id = %receipt.receipt_id,
                                    skill_id = %receipt.skill_id,
                                    "canonical Skill usage receipt persistence failed"
                                );
                            }
                        }
                    }
                }
            })
            .expect("Runtime Skill usage writer thread must start");
        Self {
            store,
            pointer_cache,
            sender,
            accepted,
            persisted,
            dropped,
            persistence_failures,
        }
    }
}

impl RuntimeSkillUsageSink for RuntimeSkillUsageRecorder {
    fn observe(
        &self,
        invocation: &SkillInvocation,
        skill_revision: &str,
        context: &RuntimeSkillUsageContext,
        usage: SkillUsageKind,
    ) -> Option<String> {
        if invocation.skill_id.trim().is_empty()
            || skill_revision.trim().is_empty()
            || context.workspace_identity.trim().is_empty()
            || context.workload_fingerprint.trim().is_empty()
            || context.config_revision.trim().is_empty()
            || context.evaluation_environment.trim().is_empty()
            || context.execution_id.trim().is_empty()
            || context.session_id.trim().is_empty()
            || context.turn_id.trim().is_empty()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let receipt_id = SkillUsageReceipt::stable_id(
            &invocation.skill_id,
            skill_revision,
            usage,
            &context.workspace_identity,
            &context.workload_fingerprint,
            &context.config_revision,
            &context.evaluation_environment,
            &context.execution_id,
            &context.session_id,
            &context.turn_id,
        );
        let receipt = SkillUsageReceipt {
            receipt_id: receipt_id.clone(),
            skill_id: invocation.skill_id.clone(),
            skill_revision: skill_revision.to_string(),
            adapter: invocation.adapter,
            usage,
            workspace_identity: context.workspace_identity.clone(),
            workload_fingerprint: context.workload_fingerprint.clone(),
            config_revision: context.config_revision.clone(),
            evaluation_environment: context.evaluation_environment.clone(),
            execution_id: context.execution_id.clone(),
            session_id: context.session_id.clone(),
            turn_id: context.turn_id.clone(),
            observed_at_ms: context.observed_at_ms,
            schema_version: SKILL_USAGE_RECEIPT_SCHEMA_VERSION,
        };
        match self.sender.try_send(receipt) {
            Ok(()) => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                Some(receipt_id)
            }
            Err(mpsc::TrySendError::Full(_)) | Err(mpsc::TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn health(&self) -> RuntimeSkillUsageSinkHealth {
        RuntimeSkillUsageSinkHealth {
            accepted: self.accepted.load(Ordering::Relaxed),
            persisted: self.persisted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            persistence_failures: self.persistence_failures.load(Ordering::Relaxed),
        }
    }

    fn active_pointer(
        &self,
        skill_id: &str,
    ) -> Result<Option<harness_contract::skill::SkillActivePointer>, String> {
        self.pointer_cache.pointer(&self.store, skill_id)
    }
}

pub(crate) fn skill_usage_stream_prefix(skill_id: &str) -> String {
    format!("skill-usage-v1:{:x}:", Sha256::digest(skill_id.as_bytes()))
}

fn persist_receipt(store: &RuntimeEventStore, receipt: &SkillUsageReceipt) -> Result<(), String> {
    let stream = format!(
        "{}{}",
        skill_usage_stream_prefix(&receipt.skill_id),
        receipt.receipt_id
    );
    if store
        .event_by_idempotency_key(&stream, &receipt.receipt_id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(());
    }
    let revision = store
        .stream_revision(&stream)
        .map_err(|error| error.to_string())?;
    store
        .append_batch_if_revision(
            stream.clone(),
            revision,
            format!("skill-usage-receipt:{}", receipt.receipt_id),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream,
                    scope: RuntimeEventScope::Skill,
                    kind: SKILL_USAGE_RECEIPT_EVENT_KIND.to_string(),
                    status: Some("observed".to_string()),
                    actor: Some("runtime.skill_usage".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "skill".to_string(),
                            id: receipt.skill_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "execution".to_string(),
                            id: receipt.execution_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "session".to_string(),
                            id: receipt.session_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: receipt.turn_id.clone(),
                        },
                    ],
                    payload: serde_json::json!({"receipt": receipt}),
                },
                idempotency_key: Some(receipt.receipt_id.clone()),
                schema_version: SKILL_USAGE_RECEIPT_SCHEMA_VERSION,
            }],
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::skill::SkillAdapterKind;
    use std::time::{Duration, Instant};

    fn invocation() -> SkillInvocation {
        SkillInvocation {
            skill_id: "review".to_string(),
            skill_version: Some("1.0.0".to_string()),
            adapter: SkillAdapterKind::PromptOnly,
            entrypoint: None,
        }
    }

    fn context() -> RuntimeSkillUsageContext {
        RuntimeSkillUsageContext {
            workspace_identity: "workspace".to_string(),
            workload_fingerprint: "workload".to_string(),
            config_revision: "config".to_string(),
            evaluation_environment: "production".to_string(),
            execution_id: "execution".to_string(),
            session_id: "session".to_string(),
            turn_id: "turn".to_string(),
            observed_at_ms: 1,
        }
    }

    #[test]
    fn canonical_receipt_is_non_blocking_scoped_and_idempotent() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let recorder = RuntimeSkillUsageRecorder::new(Arc::clone(&store));
        let first = recorder
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Hit)
            .expect("accepted");
        let duplicate = recorder
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Hit)
            .expect("accepted duplicate");
        assert_eq!(first, duplicate);

        let deadline = Instant::now() + Duration::from_secs(2);
        let events = loop {
            let events = store
                .list_scope(RuntimeEventScope::Skill, 10)
                .expect("events");
            if !events.is_empty() || Instant::now() >= deadline {
                break events;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == SKILL_USAGE_RECEIPT_EVENT_KIND)
                .count(),
            1
        );
        let receipt: SkillUsageReceipt =
            serde_json::from_value(events[0].payload["receipt"].clone()).expect("receipt");
        assert_eq!(receipt.workspace_identity, "workspace");
        assert_eq!(receipt.execution_id, "execution");
        assert!(recorder.health().persisted >= 2);
    }

    #[test]
    fn incomplete_context_is_dropped_before_persistence() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let recorder = RuntimeSkillUsageRecorder::new(Arc::clone(&store));
        let mut invalid = context();
        invalid.execution_id.clear();
        assert!(recorder
            .observe(&invocation(), "1.0.0", &invalid, SkillUsageKind::Failure)
            .is_none());
        assert_eq!(recorder.health().dropped, 1);
        assert!(store
            .list_scope(RuntimeEventScope::Skill, 10)
            .expect("events")
            .is_empty());
    }
}
