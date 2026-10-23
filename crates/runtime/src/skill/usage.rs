//! Runtime-owned, non-blocking Skill usage receipt writer.
//!
//! Gateway may observe cache mechanics while implementing the instruction
//! source, but it never receives the EventStore and cannot mint an
//! authoritative usage event.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc, Arc, Mutex,
};

use harness_contract::skill::{
    SKILL_USAGE_RECEIPT_SCHEMA_VERSION, SkillUsageKind, SkillUsageReceipt,
};
use sha2::{Digest, Sha256};

use super::{
    RuntimeSkillUsageContext, RuntimeSkillUsageSink, RuntimeSkillUsageSinkHealth, SkillInvocation,
    governance::SkillRevisionPointerCache,
};
use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};

pub const SKILL_USAGE_RECEIPT_EVENT_KIND: &str = "skill.usage.receipt.v1";
const SKILL_USAGE_QUEUE_CAPACITY: usize = 2_048;
const SKILL_USAGE_BATCH_LIMIT: usize = 128;

/// A closed intake and a physically joined worker are distinct from durable
/// receipts. A failed/unknown append can never produce a clean drain report.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SkillUsageShutdownReport {
    pub intake_closed: bool,
    pub worker_joined: bool,
    pub worker_panicked: bool,
    pub join_failed: bool,
    pub accepted: u64,
    pub persisted: u64,
    pub dropped: u64,
    pub persistence_failures: u64,
    pub unsettled_unknown: u64,
    pub clean: bool,
}

#[derive(Debug)]
pub struct RuntimeSkillUsageRecorder {
    store: Arc<RuntimeEventStore>,
    pointer_cache: Arc<SkillRevisionPointerCache>,
    sender: Mutex<Option<mpsc::SyncSender<SkillUsageReceipt>>>,
    worker: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    join_task: Mutex<Option<tokio::task::JoinHandle<bool>>>,
    worker_joined: std::sync::atomic::AtomicBool,
    worker_panicked: std::sync::atomic::AtomicBool,
    join_failed: std::sync::atomic::AtomicBool,
    accepted: Arc<AtomicU64>,
    persisted: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    persistence_failures: Arc<AtomicU64>,
}

impl RuntimeSkillUsageRecorder {
    pub fn new(store: Arc<RuntimeEventStore>) -> std::io::Result<Self> {
        Self::with_pointer_cache(store, Arc::new(SkillRevisionPointerCache::default()))
    }

    pub fn with_pointer_cache(
        store: Arc<RuntimeEventStore>,
        pointer_cache: Arc<SkillRevisionPointerCache>,
    ) -> std::io::Result<Self> {
        Self::start(
            store,
            pointer_cache,
            SKILL_USAGE_QUEUE_CAPACITY,
            persist_receipt,
            |work| {
                std::thread::Builder::new()
                    .name("cowd-runtime-skill-usage".into())
                    .spawn(work)
            },
        )
    }

    // The only production writer is persist_receipt. These private seams permit
    // short deterministic IO/panic/start fault tests without another runtime.
    fn start(
        store: Arc<RuntimeEventStore>,
        pointer_cache: Arc<SkillRevisionPointerCache>,
        capacity: usize,
        persist: impl Fn(&RuntimeEventStore, &SkillUsageReceipt) -> Result<(), String> + Send + 'static,
        spawn: impl FnOnce(Box<dyn FnOnce() + Send>) -> std::io::Result<std::thread::JoinHandle<()>>,
    ) -> std::io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<SkillUsageReceipt>(capacity);
        let accepted = Arc::new(AtomicU64::new(0));
        let persisted = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let persistence_failures = Arc::new(AtomicU64::new(0));
        let worker_persisted = Arc::clone(&persisted);
        let worker_failures = Arc::clone(&persistence_failures);
        let worker_store = Arc::clone(&store);
        let worker = spawn(Box::new(move || {
            while let Ok(first) = receiver.recv() {
                let mut batch = Vec::with_capacity(SKILL_USAGE_BATCH_LIMIT);
                batch.push(first);
                while batch.len() < SKILL_USAGE_BATCH_LIMIT {
                    match receiver.try_recv() {
                        Ok(receipt) => batch.push(receipt),
                        Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                for receipt in batch {
                    match persist(&worker_store, &receipt) {
                        Ok(()) => {
                            worker_persisted.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(error) => {
                            worker_failures.fetch_add(1, Ordering::SeqCst);
                            tracing::warn!(%error, receipt_id = %receipt.receipt_id,
                                skill_id = %receipt.skill_id, "canonical Skill usage receipt persistence failed or unknown");
                        }
                    }
                }
            }
        }))?;
        Ok(Self {
            store,
            pointer_cache,
            sender: Mutex::new(Some(sender)),
            worker: Arc::new(Mutex::new(Some(worker))),
            join_task: Mutex::new(None),
            worker_joined: false.into(),
            worker_panicked: false.into(),
            join_failed: false.into(),
            accepted,
            persisted,
            dropped,
            persistence_failures,
        })
    }

    /// Fence through the same lock as try_send. Surviving observer Arc handles
    /// cannot keep a sender clone alive or admit after this returns.
    pub fn close_intake(&self) {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    fn reap_finished_worker(&self) {
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut join_task = self
            .join_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if join_task.is_none()
            && !self.join_failed.load(Ordering::SeqCst)
            && worker
                .as_ref()
                .is_some_and(std::thread::JoinHandle::is_finished)
        {
            // is_finished is only a scheduling hint; actual join is physical
            // proof. Keep the original handle here until the IO job STARTS, so
            // cancelling a queued IO job cannot drop the only retained handle.
            let worker = Arc::clone(&self.worker);
            *join_task = Some(tokio::task::spawn_blocking(move || {
                let handle = worker
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("original Skill worker");
                handle.join().is_err()
            }));
        }
        if let Some(task) = join_task.as_mut() {
            let waker = futures::task::noop_waker();
            let mut context = std::task::Context::from_waker(&waker);
            if let std::task::Poll::Ready(result) =
                std::future::Future::poll(std::pin::Pin::new(task), &mut context)
            {
                match result {
                    Ok(panicked) => {
                        self.worker_panicked.store(panicked, Ordering::SeqCst);
                        self.worker_joined.store(true, Ordering::SeqCst);
                    }
                    Err(_) => self.join_failed.store(true, Ordering::SeqCst),
                }
                join_task.take();
            }
        }
    }

    /// Never block a Tokio thread in join on a live IO worker. The retained
    /// handle stays with this owner after deadline; callers can later reap it.
    pub async fn shutdown_and_drain(
        &self,
        timeout: std::time::Duration,
    ) -> SkillUsageShutdownReport {
        self.close_intake();
        // A new explicit drain may retry only a join job cancelled before it
        // took the original handle; it never retries the usage/Provider action.
        if self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            self.join_failed.store(false, Ordering::SeqCst);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            self.reap_finished_worker();
            if self.worker_joined.load(Ordering::SeqCst)
                || self.join_failed.load(Ordering::SeqCst)
                || tokio::time::Instant::now() >= deadline
            {
                break;
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + std::time::Duration::from_millis(5)).min(deadline),
            )
            .await;
        }
        let health = self.health();
        let worker_joined = self.worker_joined.load(Ordering::SeqCst);
        let worker_panicked = self.worker_panicked.load(Ordering::SeqCst);
        let join_failed = self.join_failed.load(Ordering::SeqCst);
        let unsettled_unknown = health.accepted.saturating_sub(health.persisted);
        SkillUsageShutdownReport {
            intake_closed: true,
            worker_joined,
            worker_panicked,
            join_failed,
            accepted: health.accepted,
            persisted: health.persisted,
            dropped: health.dropped,
            persistence_failures: health.persistence_failures,
            unsettled_unknown,
            clean: worker_joined
                && !worker_panicked
                && !join_failed
                && unsettled_unknown == 0
                && health.persistence_failures == 0,
        }
    }
}

impl Drop for RuntimeSkillUsageRecorder {
    fn drop(&mut self) {
        self.close_intake();
        if !self.worker_joined.load(Ordering::SeqCst) {
            tracing::warn!("Skill usage recorder dropped without a physically joined worker; drain remains unproven");
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
            self.dropped.fetch_add(1, Ordering::SeqCst);
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
        let sender = match self.sender.try_lock() {
            Ok(sender) => sender,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                return None;
            }
        };
        let Some(sender) = sender.as_ref() else {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            return None;
        };
        match sender.try_send(receipt) {
            Ok(()) => {
                self.accepted.fetch_add(1, Ordering::SeqCst);
                Some(receipt_id)
            }
            Err(mpsc::TrySendError::Full(_)) | Err(mpsc::TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                None
            }
        }
    }

    fn health(&self) -> RuntimeSkillUsageSinkHealth {
        RuntimeSkillUsageSinkHealth {
            accepted: self.accepted.load(Ordering::SeqCst),
            persisted: self.persisted.load(Ordering::SeqCst),
            dropped: self.dropped.load(Ordering::SeqCst),
            persistence_failures: self.persistence_failures.load(Ordering::SeqCst),
        }
    }

    fn durable_receipt(
        &self,
        skill_id: &str,
        receipt_id: &str,
    ) -> Result<Option<SkillUsageReceipt>, String> {
        durable_receipt(&self.store, skill_id, receipt_id)
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
    if !receipt_fields_complete(receipt)
        || receipt.schema_version != SKILL_USAGE_RECEIPT_SCHEMA_VERSION
    {
        return Err("Skill usage input is not a complete original receipt".into());
    }
    let stream = format!(
        "{}{}",
        skill_usage_stream_prefix(&receipt.skill_id),
        receipt.receipt_id
    );
    if let Some(existing) = durable_receipt(store, &receipt.skill_id, &receipt.receipt_id)? {
        return same_receipt_identity(&existing, receipt);
    }
    let revision = store
        .stream_revision(&stream)
        .map_err(|error| error.to_string())?;
    if revision != 0 {
        return Err("Skill usage stream contains foreign or incomplete facts".into());
    }
    let append = store
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
        .map_err(|error| error.to_string());
    confirm_receipt_append(store, receipt, append)
}

fn confirm_receipt_append(
    store: &RuntimeEventStore,
    receipt: &SkillUsageReceipt,
    append: Result<(), String>,
) -> Result<(), String> {
    // An append response can be lost after commit. Read only this original
    // receipt, never create a second event or turn an unknown result into0.
    match durable_receipt(store, &receipt.skill_id, &receipt.receipt_id)? {
        Some(existing) => same_receipt_identity(&existing, receipt),
        None => Err(append
            .err()
            .unwrap_or_else(|| "Skill receipt append not durably confirmed".into())),
    }
}

fn same_receipt_identity(
    existing: &SkillUsageReceipt,
    requested: &SkillUsageReceipt,
) -> Result<(), String> {
    let mut requested = requested.clone();
    // Repeated observation time is not part of the original stable identity;
    // retain the first durable timestamp, but never borrow another scope/adapter.
    requested.observed_at_ms = existing.observed_at_ms;
    if *existing == requested {
        Ok(())
    } else {
        Err("Skill usage original receipt identity conflict".into())
    }
}

fn durable_receipt(
    store: &RuntimeEventStore,
    skill_id: &str,
    receipt_id: &str,
) -> Result<Option<SkillUsageReceipt>, String> {
    if skill_id.trim().is_empty()
        || !receipt_id.starts_with("skill-usage-")
        || receipt_id.len() != 76
    {
        return Err("invalid Skill usage receipt selector".into());
    }
    let stream = format!("{}{receipt_id}", skill_usage_stream_prefix(skill_id));
    let Some(event) = store
        .event_by_idempotency_key(&stream, receipt_id)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let receipt = canonical_receipt_event(&event)?;
    if receipt.skill_id != skill_id || receipt.receipt_id != receipt_id {
        return Err("Skill usage receipt lookup identity conflict".into());
    }
    Ok(Some(receipt))
}

pub(super) fn canonical_receipt_event(
    event: &crate::DurableRuntimeEvent,
) -> Result<SkillUsageReceipt, String> {
    let receipt: SkillUsageReceipt =
        serde_json::from_value(event.payload["receipt"].clone()).map_err(|e| e.to_string())?;
    let stream = format!(
        "{}{}",
        skill_usage_stream_prefix(&receipt.skill_id),
        receipt.receipt_id
    );
    let receipt_id = receipt.receipt_id.as_str();
    if !receipt_fields_complete(&receipt)
        || receipt.schema_version != SKILL_USAGE_RECEIPT_SCHEMA_VERSION
        || event.payload != serde_json::json!({"receipt":receipt})
        || event.stream_id != stream
        || event.sequence != 1
        || event.transaction_index != 0
        || event.kind != SKILL_USAGE_RECEIPT_EVENT_KIND
        || event.scope != RuntimeEventScope::Skill
        || event.actor.as_deref() != Some("runtime.skill_usage")
        || event.status.as_deref() != Some("observed")
        || event.schema_version != SKILL_USAGE_RECEIPT_SCHEMA_VERSION
        || event.idempotency_key.as_deref() != Some(receipt_id)
        || event.transaction_id != format!("skill-usage-receipt:{receipt_id}")
        || event.refs
            != vec![
                RuntimeEventRef {
                    kind: "skill".into(),
                    id: receipt.skill_id.clone(),
                },
                RuntimeEventRef {
                    kind: "execution".into(),
                    id: receipt.execution_id.clone(),
                },
                RuntimeEventRef {
                    kind: "session".into(),
                    id: receipt.session_id.clone(),
                },
                RuntimeEventRef {
                    kind: "turn".into(),
                    id: receipt.turn_id.clone(),
                },
            ]
    {
        return Err("unverified original Skill usage receipt".into());
    }
    Ok(receipt)
}

fn receipt_fields_complete(receipt: &SkillUsageReceipt) -> bool {
    [
        receipt.receipt_id.as_str(),
        receipt.skill_id.as_str(),
        receipt.skill_revision.as_str(),
        receipt.workspace_identity.as_str(),
        receipt.workload_fingerprint.as_str(),
        receipt.config_revision.as_str(),
        receipt.evaluation_environment.as_str(),
        receipt.execution_id.as_str(),
        receipt.session_id.as_str(),
        receipt.turn_id.as_str(),
    ]
    .iter()
    .all(|value| !value.trim().is_empty())
        && receipt.receipt_id
            == SkillUsageReceipt::stable_id(
                &receipt.skill_id,
                &receipt.skill_revision,
                receipt.usage,
                &receipt.workspace_identity,
                &receipt.workload_fingerprint,
                &receipt.config_revision,
                &receipt.evaluation_environment,
                &receipt.execution_id,
                &receipt.session_id,
                &receipt.turn_id,
            )
}

#[cfg(test)]
pub(super) fn persist_receipt_for_test(
    store: &RuntimeEventStore,
    receipt: &SkillUsageReceipt,
) -> Result<(), String> {
    persist_receipt(store, receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::skill::SkillAdapterKind;
    use std::time::Duration;

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

    #[tokio::test]
    async fn canonical_receipt_is_non_blocking_scoped_and_idempotent() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let recorder = RuntimeSkillUsageRecorder::new(Arc::clone(&store)).unwrap();
        let first = recorder
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Hit)
            .expect("accepted");
        let duplicate = recorder
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Hit)
            .expect("accepted duplicate");
        assert_eq!(first, duplicate);

        let drained = recorder.shutdown_and_drain(Duration::from_secs(2)).await;
        assert!(drained.clean, "{drained:?}");
        assert_eq!((drained.accepted, drained.persisted), (2, 2));
        let events = store.list_scope(RuntimeEventScope::Skill, 10).unwrap();
        let durable = recorder.durable_receipt("review", &first).unwrap().unwrap();
        assert_eq!(durable.workspace_identity, "workspace");
        assert_eq!(durable.turn_id, "turn");
        assert!(recorder
            .durable_receipt("foreign", &first)
            .unwrap()
            .is_none());
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

    #[tokio::test]
    async fn contended_usage_intake_drops_without_waiting_or_claiming_durability() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let recorder = RuntimeSkillUsageRecorder::new(Arc::clone(&store)).unwrap();
        let guard = recorder.sender.lock().unwrap();
        assert!(recorder
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Load)
            .is_none());
        assert_eq!(recorder.health().accepted, 0);
        assert_eq!(recorder.health().dropped, 1);
        drop(guard);
        assert!(
            recorder
                .shutdown_and_drain(Duration::from_secs(2))
                .await
                .clean
        );
        assert!(store
            .list_scope(RuntimeEventScope::Skill, 10)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn incomplete_context_is_dropped_before_persistence() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let recorder = RuntimeSkillUsageRecorder::new(Arc::clone(&store)).unwrap();
        let mut invalid = context();
        invalid.execution_id.clear();
        assert!(recorder
            .observe(&invocation(), "1.0.0", &invalid, SkillUsageKind::Failure)
            .is_none());
        assert_eq!(recorder.health().dropped, 1);
        assert!(
            recorder
                .shutdown_and_drain(Duration::from_secs(2))
                .await
                .clean
        );
        assert!(store
            .list_scope(RuntimeEventScope::Skill, 10)
            .expect("events")
            .is_empty());
    }
    fn receipt() -> SkillUsageReceipt {
        let invocation = invocation();
        let context = context();
        SkillUsageReceipt {
            receipt_id: SkillUsageReceipt::stable_id(
                "review",
                "1.0.0",
                SkillUsageKind::Load,
                "workspace",
                "workload",
                "config",
                "production",
                "execution",
                "session",
                "turn",
            ),
            skill_id: invocation.skill_id,
            skill_revision: "1.0.0".into(),
            adapter: invocation.adapter,
            usage: SkillUsageKind::Load,
            workspace_identity: context.workspace_identity,
            workload_fingerprint: context.workload_fingerprint,
            config_revision: context.config_revision,
            evaluation_environment: context.evaluation_environment,
            execution_id: context.execution_id,
            session_id: context.session_id,
            turn_id: context.turn_id,
            observed_at_ms: 1,
            schema_version: SKILL_USAGE_RECEIPT_SCHEMA_VERSION,
        }
    }

    #[tokio::test]
    async fn usage_close_fences_surviving_observers_and_timeout_keeps_the_original_worker() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered_tx));
        let exited = Mutex::new(Some(exited_tx));
        let (release, wait_release) = mpsc::channel();
        let recorder = Arc::new(
            RuntimeSkillUsageRecorder::start(
                store.clone(),
                Arc::new(SkillRevisionPointerCache::default()),
                1,
                move |store, receipt| {
                    if let Some(entered) = entered.lock().unwrap().take() {
                        entered.send(()).unwrap();
                        wait_release.recv().unwrap();
                    }
                    persist_receipt(store, receipt)?;
                    if let Some(exited) = exited.lock().unwrap().take() {
                        exited.send(()).unwrap();
                    }
                    Ok(())
                },
                |work| std::thread::Builder::new().spawn(work),
            )
            .unwrap(),
        );
        let observer: Arc<dyn RuntimeSkillUsageSink> = recorder.clone();
        let first = observer
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Load)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            observer
                .durable_receipt("review", &first)
                .unwrap()
                .is_none(),
            "accepted is not durable"
        );
        let mut second_context = context();
        second_context.turn_id = "second".into();
        let second = observer
            .observe(
                &invocation(),
                "1.0.0",
                &second_context,
                SkillUsageKind::Load,
            )
            .unwrap();
        let mut overflow = context();
        overflow.turn_id = "third".into();
        assert!(observer
            .observe(&invocation(), "1.0.0", &overflow, SkillUsageKind::Load)
            .is_none());
        let timed_out = recorder.shutdown_and_drain(Duration::ZERO).await;
        assert!(!timed_out.clean && !timed_out.worker_joined);
        assert_eq!(timed_out.unsettled_unknown, 2);
        assert!(observer
            .observe(&invocation(), "1.0.0", &overflow, SkillUsageKind::Load)
            .is_none());
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), exited_rx)
            .await
            .unwrap()
            .unwrap();
        let drained = recorder.shutdown_and_drain(Duration::from_secs(2)).await;
        assert!(drained.clean, "{drained:?}");
        assert!(drained.worker_joined && !drained.worker_panicked);
        assert_eq!(
            (
                drained.accepted,
                drained.persisted,
                drained.unsettled_unknown
            ),
            (2, 2, 0)
        );
        assert_eq!(
            observer
                .durable_receipt("review", &second)
                .unwrap()
                .unwrap()
                .turn_id,
            "second"
        );
        assert_eq!(
            store
                .list_scope(RuntimeEventScope::Skill, 10)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn usage_start_failure_io_failure_and_worker_panic_cannot_be_clean() {
        let failed = RuntimeSkillUsageRecorder::start(
            Arc::new(RuntimeEventStore::for_test()),
            Arc::new(SkillRevisionPointerCache::default()),
            1,
            persist_receipt,
            |_| Err(std::io::Error::other("controlled thread start refusal")),
        );
        assert!(failed.is_err());
        for panic_worker in [false, true] {
            let recorder = RuntimeSkillUsageRecorder::start(
                Arc::new(RuntimeEventStore::for_test()),
                Arc::new(SkillRevisionPointerCache::default()),
                1,
                move |_, _| {
                    assert!(!panic_worker, "controlled usage worker panic");
                    Err("controlled append not committed".into())
                },
                |work| std::thread::Builder::new().spawn(work),
            )
            .unwrap();
            let id = recorder
                .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Load)
                .unwrap();
            let report = recorder.shutdown_and_drain(Duration::from_secs(2)).await;
            assert!(!report.clean && report.worker_joined);
            assert_eq!(report.worker_panicked, panic_worker);
            assert_eq!(report.unsettled_unknown, 1);
            assert_eq!(
                report.persistence_failures,
                if panic_worker { 0 } else { 1 }
            );
            assert!(recorder.durable_receipt("review", &id).unwrap().is_none());
        }
    }

    #[test]
    fn usage_exact_committed_receipt_resolves_response_loss_and_reread_without_second_fact() {
        let store = RuntimeEventStore::for_test();
        let expected = receipt();
        assert!(confirm_receipt_append(&store, &expected, Err("not committed".into())).is_err());
        persist_receipt(&store, &expected).unwrap();
        assert!(
            confirm_receipt_append(&store, &expected, Err("committed response lost".into()))
                .is_ok()
        );
        let mut later = expected.clone();
        later.observed_at_ms = 2;
        persist_receipt(&store, &later).unwrap();
        assert_eq!(
            durable_receipt(&store, "review", &expected.receipt_id).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            store
                .list_scope(RuntimeEventScope::Skill, 10)
                .unwrap()
                .len(),
            1
        );
        let mut foreign = expected;
        foreign.workspace_identity = "foreign".into();
        assert!(persist_receipt(&store, &foreign).is_err());
        assert_eq!(
            store
                .list_scope(RuntimeEventScope::Skill, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn original_runtime_owns_one_lazy_usage_worker_and_shutdown_fences_its_port() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let first = services.skill_usage_recorder().unwrap();
        let second = services.skill_usage_recorder().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let id = first
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Load)
            .unwrap();
        let report = services
            .shutdown_skill_usage(Duration::from_secs(2))
            .await
            .unwrap();
        assert!(report.clean, "{report:?}");
        assert!(services.skill_usage_recorder().is_err());
        assert!(second
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Load)
            .is_none());
        assert_eq!(
            first
                .durable_receipt("review", &id)
                .unwrap()
                .unwrap()
                .workspace_identity,
            "workspace"
        );
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }
    #[tokio::test]
    async fn concurrent_usage_observers_cannot_cross_the_closed_intake_fence() {
        let recorder = Arc::new(
            RuntimeSkillUsageRecorder::new(Arc::new(RuntimeEventStore::for_test())).unwrap(),
        );
        let first = recorder
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Load)
            .unwrap();
        let start = Arc::new(std::sync::Barrier::new(5));
        let mut observers = Vec::new();
        for index in 0..4 {
            let recorder = recorder.clone();
            let start = start.clone();
            observers.push(std::thread::spawn(move || {
                start.wait();
                let mut accepted = Vec::new();
                for sample in 0..16 {
                    let mut context = context();
                    context.turn_id = format!("racing-{index}-{sample}");
                    if let Some(id) =
                        recorder.observe(&invocation(), "1.0.0", &context, SkillUsageKind::Load)
                    {
                        accepted.push(id);
                    }
                }
                accepted
            }));
        }
        start.wait();
        recorder.close_intake();
        let mut accepted = vec![first];
        for observer in observers {
            accepted.extend(observer.join().unwrap());
        }
        assert!(recorder
            .observe(&invocation(), "1.0.0", &context(), SkillUsageKind::Load)
            .is_none());
        let report = recorder.shutdown_and_drain(Duration::from_secs(2)).await;
        assert!(report.clean, "{report:?}");
        assert_eq!(report.accepted, accepted.len() as u64);
        assert_eq!(report.persisted, accepted.len() as u64);
        for id in accepted {
            assert!(recorder.durable_receipt("review", &id).unwrap().is_some());
        }
    }
    #[test]
    fn canonical_usage_reader_rejects_each_foreign_provenance_field() {
        let store = RuntimeEventStore::for_test();
        let receipt = receipt();
        persist_receipt(&store, &receipt).unwrap();
        let stream = format!(
            "{}{}",
            skill_usage_stream_prefix("review"),
            receipt.receipt_id
        );
        let event = store
            .event_by_idempotency_key(&stream, &receipt.receipt_id)
            .unwrap()
            .unwrap();
        assert_eq!(canonical_receipt_event(&event).unwrap(), receipt);
        for defect in ["actor", "stream", "refs", "schema", "payload", "position"] {
            let mut changed = event.clone();
            match defect {
                "actor" => changed.actor = Some("foreign".into()),
                "stream" => changed.stream_id = "foreign".into(),
                "refs" => changed.refs[1].id = "foreign-execution".into(),
                "schema" => changed.schema_version = SKILL_USAGE_RECEIPT_SCHEMA_VERSION + 1,
                "payload" => changed.payload["receipt"]["workspace_identity"] = "foreign".into(),
                "position" => changed.transaction_index = 1,
                _ => unreachable!(),
            }
            assert!(canonical_receipt_event(&changed).is_err(), "{defect}");
        }
        let poisoned = RuntimeEventStore::for_test();
        poisoned
            .append(RuntimeEventInput {
                stream_id: stream,
                scope: RuntimeEventScope::Skill,
                kind: "foreign.fact".into(),
                status: None,
                actor: Some("foreign".into()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .unwrap();
        assert!(persist_receipt(&poisoned, &receipt).is_err());
        assert_eq!(
            poisoned
                .list_scope(RuntimeEventScope::Skill, 10)
                .unwrap()
                .len(),
            1,
            "no receipt appended after foreign stream history"
        );
    }
}
