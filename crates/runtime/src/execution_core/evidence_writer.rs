use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use uuid::Uuid;

use super::graph::{ResourceAdmissionObservation, ResourceAdmissionObservationStatus};
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

const TRANSITION_QUEUE_CAPACITY: usize = 2_048;
const PRIORITY_QUEUE_CAPACITY: usize = 8_192;
const PRIORITY_DRAIN_BATCH: usize = 256;
const COALESCIBLE_SETTLE_WINDOW: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceEvidenceWriterHealth {
    pub published: u64,
    pub coalesced: u64,
    pub coalescible_dropped: u64,
    pub priority_dropped: u64,
    pub persistence_failures: u64,
}

/// Non-blocking bridge from the admission lock path to durable evidence.
///
/// Queue/waiting transitions are coalesced by request in a bounded map.
/// Grant/defer/overload transitions use a larger priority channel. All
/// admission observations are reconstructible telemetry, so saturation is
/// measured and dropped instead of blocking admission or growing memory.
pub struct ResourceEvidenceWriter {
    wake: Mutex<Option<mpsc::SyncSender<()>>>,
    priority: Mutex<Option<mpsc::SyncSender<ResourceAdmissionObservation>>>,
    pending: Arc<Mutex<HashMap<Uuid, ResourceAdmissionObservation>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    published: Arc<AtomicU64>,
    coalesced: AtomicU64,
    coalescible_dropped: AtomicU64,
    priority_dropped: AtomicU64,
    persistence_failures: Arc<AtomicU64>,
}

impl ResourceEvidenceWriter {
    #[must_use]
    pub fn start(store: Arc<RuntimeEventStore>) -> Arc<Self> {
        let (wake_tx, wake_rx) = mpsc::sync_channel(1);
        let (priority_tx, priority_rx) = mpsc::sync_channel(PRIORITY_QUEUE_CAPACITY);
        let pending = Arc::new(Mutex::new(
            HashMap::<Uuid, ResourceAdmissionObservation>::new(),
        ));
        let published = Arc::new(AtomicU64::new(0));
        let persistence_failures = Arc::new(AtomicU64::new(0));
        let worker_pending = Arc::clone(&pending);
        let worker_published = Arc::clone(&published);
        let worker_failures = Arc::clone(&persistence_failures);
        let worker = std::thread::Builder::new()
            .name("cowd-resource-evidence".to_string())
            .spawn(move || {
                loop {
                    drain_priority(
                        &store,
                        &priority_rx,
                        &worker_published,
                        &worker_failures,
                        PRIORITY_DRAIN_BATCH,
                    );
                    match wake_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(()) => {
                            // A wake only signals that coalescible state exists.
                            // Give same-request bursts a bounded settling window
                            // so the durable stream records the latest state once.
                            let disconnected = loop {
                                match wake_rx.recv_timeout(COALESCIBLE_SETTLE_WINDOW) {
                                    Ok(()) => {}
                                    Err(mpsc::RecvTimeoutError::Timeout) => break false,
                                    Err(mpsc::RecvTimeoutError::Disconnected) => break true,
                                }
                            };
                            drain_coalesced(
                                &store,
                                &worker_pending,
                                &worker_published,
                                &worker_failures,
                            );
                            if disconnected {
                                break;
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            drain_coalesced(
                                &store,
                                &worker_pending,
                                &worker_published,
                                &worker_failures,
                            );
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                while drain_priority(
                    &store,
                    &priority_rx,
                    &worker_published,
                    &worker_failures,
                    PRIORITY_DRAIN_BATCH,
                ) > 0
                {}
                drain_coalesced(&store, &worker_pending, &worker_published, &worker_failures);
            })
            .expect("resource evidence writer thread must start");
        Arc::new(Self {
            wake: Mutex::new(Some(wake_tx)),
            priority: Mutex::new(Some(priority_tx)),
            pending,
            worker: Mutex::new(Some(worker)),
            published,
            coalesced: AtomicU64::new(0),
            coalescible_dropped: AtomicU64::new(0),
            priority_dropped: AtomicU64::new(0),
            persistence_failures,
        })
    }

    pub fn try_publish(&self, observation: &ResourceAdmissionObservation) {
        let priority = matches!(
            observation.status,
            ResourceAdmissionObservationStatus::Granted
                | ResourceAdmissionObservationStatus::Deferred
                | ResourceAdmissionObservationStatus::Overloaded
        );
        if priority {
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&observation.request_id);
            if let Some(sender) = self
                .priority
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            {
                if sender.try_send(observation.clone()).is_err() {
                    // Resource observations are reconstructible telemetry, not
                    // the canonical execution terminal. Never block admission
                    // or allow a database outage to grow process memory.
                    self.priority_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            return;
        }

        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pending.contains_key(&observation.request_id) {
            self.coalesced.fetch_add(1, Ordering::Relaxed);
        } else if pending.len() >= TRANSITION_QUEUE_CAPACITY {
            self.coalescible_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        pending.insert(observation.request_id, observation.clone());
        drop(pending);

        if let Some(sender) = self
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            let _ = sender.try_send(());
        }
    }

    #[must_use]
    pub fn health(&self) -> ResourceEvidenceWriterHealth {
        ResourceEvidenceWriterHealth {
            published: self.published.load(Ordering::Relaxed),
            coalesced: self.coalesced.load(Ordering::Relaxed),
            coalescible_dropped: self.coalescible_dropped.load(Ordering::Relaxed),
            priority_dropped: self.priority_dropped.load(Ordering::Relaxed),
            persistence_failures: self.persistence_failures.load(Ordering::Relaxed),
        }
    }

    pub fn shutdown_and_drain(&self) {
        self.wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.priority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = worker.join();
        }
    }
}

fn drain_priority(
    store: &RuntimeEventStore,
    receiver: &mpsc::Receiver<ResourceAdmissionObservation>,
    published: &AtomicU64,
    failures: &AtomicU64,
    limit: usize,
) -> usize {
    let mut drained = 0;
    while drained < limit {
        let Ok(observation) = receiver.try_recv() else {
            break;
        };
        persist_and_count(store, &observation, published, failures);
        drained += 1;
    }
    drained
}

fn drain_coalesced(
    store: &RuntimeEventStore,
    pending: &Mutex<HashMap<Uuid, ResourceAdmissionObservation>>,
    published: &AtomicU64,
    failures: &AtomicU64,
) {
    let batch = {
        let mut pending = pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pending
            .drain()
            .map(|(_, observation)| observation)
            .collect::<Vec<_>>()
    };
    for observation in batch {
        persist_and_count(store, &observation, published, failures);
    }
}

fn persist_and_count(
    store: &RuntimeEventStore,
    observation: &ResourceAdmissionObservation,
    published: &AtomicU64,
    failures: &AtomicU64,
) {
    if persist_observation(store, observation).is_ok() {
        published.fetch_add(1, Ordering::Relaxed);
    } else {
        failures.fetch_add(1, Ordering::Relaxed);
    }
}

fn persist_observation(
    store: &RuntimeEventStore,
    observation: &ResourceAdmissionObservation,
) -> Result<(), String> {
    let mut refs = vec![RuntimeEventRef {
        kind: "resource_request".to_string(),
        id: observation.request_id.to_string(),
    }];
    if let Some(execution_id) = observation.fairness_key.strip_prefix("graph:") {
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: execution_id.to_string(),
        });
    } else if let Some(session_id) = observation.fairness_key.strip_prefix("session:") {
        refs.push(RuntimeEventRef {
            kind: "session".to_string(),
            id: session_id.to_string(),
        });
    }
    let state = match observation.status {
        ResourceAdmissionObservationStatus::Queued => "queued",
        ResourceAdmissionObservationStatus::Waiting => "waiting",
        ResourceAdmissionObservationStatus::Granted => "granted",
        ResourceAdmissionObservationStatus::Deferred => "deferred",
        ResourceAdmissionObservationStatus::Overloaded => "overloaded",
    };
    store
        .append(RuntimeEventInput {
            stream_id: format!("resource-admission:{}", observation.request_id),
            scope: RuntimeEventScope::Schedule,
            kind: format!("resource.admission.{state}"),
            status: Some(state.to_string()),
            actor: Some("execution_resource_manager".to_string()),
            refs,
            payload: serde_json::to_value(observation).unwrap_or_else(
                |error| serde_json::json!({ "serialization_error": error.to_string() }),
            ),
        })
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::ExecutionServiceClass;

    use super::*;
    use crate::execution_core::graph::{ExecutionResourceKind, ResourceWaitReason};

    fn observation(
        request_id: Uuid,
        status: ResourceAdmissionObservationStatus,
        queue_age_ms: u64,
    ) -> ResourceAdmissionObservation {
        ResourceAdmissionObservation {
            request_id,
            status,
            requested_priority: None,
            deadline_at_ms: None,
            requested_service_class: ExecutionServiceClass::Interactive,
            resolved_service_class: ExecutionServiceClass::Interactive,
            parent_class_ceiling: None,
            demands: vec![(ExecutionResourceKind::Provider, 1)],
            normalized_scope: None,
            fairness_key: "session:evidence-writer-test".to_string(),
            enqueue_sequence: Some(1),
            enqueued_at_ms: Some(1),
            observed_at_ms: queue_age_ms.saturating_add(1),
            queue_age_ms,
            wait_reason: (status == ResourceAdmissionObservationStatus::Waiting)
                .then_some(ResourceWaitReason::Capacity),
            blocker: None,
            policy_revision: 1,
            pending: 1,
        }
    }

    #[test]
    fn shutdown_drains_priority_and_coalesces_waiting_observations() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let writer = ResourceEvidenceWriter::start(Arc::clone(&store));
        let waiting_id = Uuid::new_v4();
        writer.try_publish(&observation(
            waiting_id,
            ResourceAdmissionObservationStatus::Waiting,
            10,
        ));
        writer.try_publish(&observation(
            waiting_id,
            ResourceAdmissionObservationStatus::Waiting,
            20,
        ));
        let granted_id = Uuid::new_v4();
        writer.try_publish(&observation(
            granted_id,
            ResourceAdmissionObservationStatus::Granted,
            30,
        ));

        writer.shutdown_and_drain();

        let waiting = store
            .list_stream(&format!("resource-admission:{waiting_id}"))
            .expect("waiting stream");
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].payload["queue_age_ms"], 20);
        let granted = store
            .list_stream(&format!("resource-admission:{granted_id}"))
            .expect("granted stream");
        assert_eq!(granted.len(), 1);
        assert_eq!(granted[0].kind, "resource.admission.granted");
        let health = writer.health();
        assert_eq!(health.coalesced, 1);
        assert_eq!(health.published, 2);
        assert_eq!(health.persistence_failures, 0);
    }

    #[test]
    fn bounded_priority_drain_leaves_a_fair_slot_for_coalesced_updates() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let (sender, receiver) = mpsc::sync_channel(PRIORITY_DRAIN_BATCH + 1);
        let published = AtomicU64::new(0);
        let failures = AtomicU64::new(0);
        for _ in 0..=PRIORITY_DRAIN_BATCH {
            sender
                .send(observation(
                    Uuid::new_v4(),
                    ResourceAdmissionObservationStatus::Granted,
                    0,
                ))
                .expect("priority observation");
        }

        assert_eq!(
            drain_priority(
                &store,
                &receiver,
                &published,
                &failures,
                PRIORITY_DRAIN_BATCH,
            ),
            PRIORITY_DRAIN_BATCH
        );
        assert_eq!(
            receiver.try_iter().count(),
            1,
            "one priority item must remain after a bounded drain so the worker can service coalesced updates"
        );
        assert_eq!(
            published.load(Ordering::Relaxed),
            PRIORITY_DRAIN_BATCH as u64
        );
        assert_eq!(failures.load(Ordering::Relaxed), 0);
    }
}
