use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use memory::UnifiedSessionStore;
use tokio::{sync::watch, task::JoinHandle};

use crate::{event_bus::SessionEventBus, runtime_service::RuntimeService};

const WORKER_BATCH: usize = 32;
const LEASE_MS: u64 = 30_000;
const MAX_ATTEMPTS: u32 = 8;

#[async_trait]
impl runtime::SessionIngressExecutor for RuntimeService {
    async fn execute_ingress(
        &self,
        record: &memory::SessionRuntimeOutboxRecord,
        content: &str,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        self.execute_ingress_record(record, content).await
    }
}

pub(crate) struct SessionRuntimeBridge {
    shutdown: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
}

impl SessionRuntimeBridge {
    pub(crate) fn start(
        runtime_service: Arc<RuntimeService>,
        store: Arc<UnifiedSessionStore>,
        event_bus: Arc<SessionEventBus>,
    ) -> Result<Self, String> {
        let router = runtime_service.session_input_router();
        let (shutdown, ingress_rx) = watch::channel(false);
        let delivery_rx = shutdown.subscribe();
        let ingress_runtime = Arc::clone(&runtime_service);
        let ingress = tokio::spawn(async move {
            run_ingress_worker(router, ingress_runtime, ingress_rx).await;
        });
        let delivery_runtime = Arc::clone(&runtime_service);
        let delivery_store = Arc::clone(delivery_runtime.runtime_services().event_store());
        let delivery = tokio::spawn(async move {
            run_delivery_worker(delivery_store, store, event_bus, delivery_rx).await;
        });
        Ok(Self {
            shutdown,
            handles: vec![ingress, delivery],
        })
    }

    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        for handle in self.handles {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
        }
    }
}

async fn run_ingress_worker(
    router: Arc<runtime::SessionInputRouter>,
    runtime_service: Arc<RuntimeService>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        match router
            .route_pending_with(runtime_service.as_ref(), WORKER_BATCH)
            .await
        {
            Ok(report) if report.claimed > 0 => tracing::debug!(
                claimed = report.claimed,
                materialized = report.materialized,
                retries = report.retry_scheduled,
                blocked = report.blocked,
                "session ingress batch processed"
            ),
            Ok(_) => {}
            Err(error) => tracing::error!(%error, "session ingress worker failed"),
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
    }
}

async fn run_delivery_worker(
    event_store: Arc<runtime::RuntimeEventStore>,
    store: Arc<UnifiedSessionStore>,
    event_bus: Arc<SessionEventBus>,
    mut shutdown: watch::Receiver<bool>,
) {
    let worker_id = format!("gateway-delivery:{}", uuid::Uuid::new_v4());
    loop {
        if *shutdown.borrow() {
            break;
        }
        let claim_store = Arc::clone(&event_store);
        let claim_worker = worker_id.clone();
        let claimed = tokio::task::spawn_blocking(move || {
            claim_store.claim_session_terminals(&claim_worker, now_ms(), LEASE_MS, WORKER_BATCH)
        })
        .await;
        match claimed {
            Ok(Ok(records)) => {
                for record in records {
                    deliver_terminal(&event_store, &store, &event_bus, &worker_id, record).await;
                }
            }
            Ok(Err(error)) => tracing::error!(%error, "terminal outbox claim failed"),
            Err(error) => tracing::error!(%error, "terminal outbox worker join failed"),
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
    }
}

async fn deliver_terminal(
    event_store: &Arc<runtime::RuntimeEventStore>,
    store: &UnifiedSessionStore,
    event_bus: &SessionEventBus,
    worker_id: &str,
    record: runtime::RuntimeSessionOutboxRecord,
) {
    let outcome = decode_terminal_payload(&record.payload_ref).and_then(|text| {
        serde_json::to_string(&serde_json::json!([{ "type": "text", "text": text }]))
            .map_err(|error| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    error.to_string(),
                )
            })
            .map(|content| (text, content))
    });
    let outcome = match outcome {
        Ok((text, content_json)) => store
            .append_terminal_message_idempotent(
                &record.message_id,
                &record.session_id,
                &content_json,
                now_ms(),
            )
            .await
            .map(|(message, inserted)| (text, message, inserted))
            .map_err(|error| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::Permanent,
                    error.to_string(),
                )
            }),
        Err(error) => Err(error),
    };
    match outcome {
        Ok((text, message, inserted)) => {
            if inserted {
                let event = serde_json::json!({
                    "type": "TurnComplete",
                    "session_id": record.session_id,
                    "message_id": record.message_id,
                    "sequence": message.sequence,
                    "response": text,
                    "committed": true,
                    "runtime_commit_cursor": record.commit_cursor,
                });
                event_bus
                    .broadcast(&record.session_id, &event.to_string())
                    .await;
            }
            let event_store = Arc::clone(event_store);
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            if let Err(error) = tokio::task::spawn_blocking(move || {
                event_store.ack_session_terminal(&terminal_id, &worker, revision, now_ms())
            })
            .await
            .unwrap_or_else(|error| {
                Err(runtime::RuntimeEventStoreError::Corrupt(error.to_string()))
            }) {
                // The durable message ID makes replay safe. Leaving the lease
                // unacked intentionally lets the next worker take it over.
                tracing::error!(terminal_id = %record.terminal_id, %error, "terminal append committed but ack failed");
            }
        }
        Err((class, error)) => {
            let event_store = Arc::clone(event_store);
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            let retry_at = now_ms().saturating_add(retry_delay_ms(record.attempts));
            if let Err(failure) = tokio::task::spawn_blocking(move || {
                event_store.fail_session_terminal(
                    &terminal_id,
                    &worker,
                    revision,
                    class,
                    &error,
                    retry_at,
                    MAX_ATTEMPTS,
                    now_ms(),
                )
            })
            .await
            .unwrap_or_else(|error| {
                Err(runtime::RuntimeEventStoreError::Corrupt(error.to_string()))
            }) {
                tracing::error!(terminal_id = %record.terminal_id, error = %failure, "terminal failure state could not be recorded");
            }
        }
    }
}

fn decode_terminal_payload(
    payload_ref: &str,
) -> Result<String, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    let encoded = payload_ref.strip_prefix("assistant_json:").ok_or_else(|| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal payload does not use assistant_json".to_string(),
        )
    })?;
    serde_json::from_str::<String>(encoded).map_err(|error| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            error.to_string(),
        )
    })
}

fn retry_delay_ms(attempt: u32) -> u64 {
    250_u64.saturating_mul(1_u64 << attempt.min(8))
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
    use memory::SessionRecord;
    use tokio::sync::mpsc;

    async fn delivery_fixture() -> (
        Arc<runtime::RuntimeEventStore>,
        Arc<UnifiedSessionStore>,
        Arc<SessionEventBus>,
        mpsc::Receiver<String>,
    ) {
        let event_store = Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap());
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "chat".to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let event_bus = SessionEventBus::new();
        let (tx, rx) = mpsc::channel(8);
        event_bus.subscribe("s1", tx).await;
        (event_store, store, event_bus, rx)
    }

    #[test]
    fn terminal_payload_requires_typed_prefix() {
        assert_eq!(
            decode_terminal_payload("assistant_json:\"done\"").unwrap(),
            "done"
        );
        assert!(decode_terminal_payload("evidence:1").is_err());
    }

    #[tokio::test]
    async fn append_success_ack_failure_replays_without_duplicate_message_or_event() {
        let (event_store, store, event_bus, mut rx) = delivery_fixture().await;
        event_store
            .enqueue_session_terminal("t1", "m1", "s1", 7, "assistant_json:\"done\"")
            .unwrap();
        let record = event_store
            .claim_session_terminals("owner-a", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();

        deliver_terminal(&event_store, &store, &event_bus, "wrong-owner", record).await;
        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        assert!(rx.try_recv().is_ok());
        assert_eq!(
            event_store.session_terminal("t1").unwrap().unwrap().status,
            "claimed"
        );

        let reclaimed = event_store
            .claim_session_terminals("owner-b", 110, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        deliver_terminal(&event_store, &store, &event_bus, "owner-b", reclaimed).await;
        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        assert!(rx.try_recv().is_err(), "replay must not rebroadcast");
        assert_eq!(
            event_store.session_terminal("t1").unwrap().unwrap().status,
            "materialized"
        );
    }

    #[tokio::test]
    async fn corrupt_terminal_is_poisoned_and_visible_to_operations() {
        let (event_store, store, event_bus, _rx) = delivery_fixture().await;
        event_store
            .enqueue_session_terminal("poison", "m2", "s1", 8, "not-typed")
            .unwrap();
        let record = event_store
            .claim_session_terminals("worker", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        deliver_terminal(&event_store, &store, &event_bus, "worker", record).await;
        let poison = event_store.blocked_session_terminals(10).unwrap();
        assert_eq!(poison.len(), 1);
        assert_eq!(poison[0].terminal_id, "poison");
        assert_eq!(poison[0].failure_class.as_deref(), Some("corrupt_payload"));
    }

    #[tokio::test]
    async fn delivery_worker_starts_and_shuts_down_gracefully() {
        let (event_store, store, event_bus, _rx) = delivery_fixture().await;
        let (shutdown, receiver) = watch::channel(false);
        let handle = tokio::spawn(run_delivery_worker(event_store, store, event_bus, receiver));
        tokio::task::yield_now().await;
        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("worker must observe graceful shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn delivery_worker_restart_materializes_terminal_exactly_once() {
        let (event_store, store, event_bus, mut rx) = delivery_fixture().await;
        event_store
            .enqueue_session_terminal(
                "restart-t1",
                "restart-m1",
                "s1",
                9,
                "assistant_json:\"done\"",
            )
            .unwrap();

        for _ in 0..2 {
            let (shutdown, receiver) = watch::channel(false);
            let handle = tokio::spawn(run_delivery_worker(
                Arc::clone(&event_store),
                Arc::clone(&store),
                Arc::clone(&event_bus),
                receiver,
            ));
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if event_store
                        .session_terminal("restart-t1")
                        .unwrap()
                        .is_some_and(|record| record.status == "materialized")
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("worker must materialize the durable terminal");
            shutdown.send(true).unwrap();
            handle.await.unwrap();
        }

        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        assert!(rx.try_recv().is_ok());
        assert!(
            rx.try_recv().is_err(),
            "restart must not rebroadcast terminal"
        );
    }
}
