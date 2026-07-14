use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use memory::{OutboxFailureClass, SessionMissionOutboxOperation, UnifiedSessionStore};
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
        let mission_rx = shutdown.subscribe();
        let ingress_runtime = Arc::clone(&runtime_service);
        let ingress = tokio::spawn(async move {
            run_ingress_worker(router, ingress_runtime, ingress_rx).await;
        });
        let delivery_runtime = Arc::clone(&runtime_service);
        let delivery_store = delivery_runtime
            .runtime_services()
            .session_terminal_delivery();
        let delivery = tokio::spawn(async move {
            run_delivery_worker(delivery_store, store, event_bus, delivery_rx).await;
        });
        let mission_store = runtime_service
            .session_kernel()
            .unified_store()
            .ok_or_else(|| "mission bridge requires UnifiedSessionStore".to_string())?;
        let mission_runtime = Arc::clone(runtime_service.runtime_services().mission_runtime());
        let workspace_key = runtime_service
            .runtime_services()
            .workspace_key()
            .to_string();
        let mission = tokio::spawn(async move {
            run_mission_membership_worker(
                mission_store,
                mission_runtime,
                workspace_key,
                mission_rx,
            )
            .await;
        });
        Ok(Self {
            shutdown,
            handles: vec![ingress, delivery, mission],
        })
    }

    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        for handle in self.handles {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
        }
    }
}

async fn run_mission_membership_worker(
    store: Arc<UnifiedSessionStore>,
    mission: Arc<runtime::MissionRuntime>,
    workspace_key: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let worker_id = format!("gateway-mission-membership:{}", uuid::Uuid::new_v4());
    loop {
        if *shutdown.borrow() {
            break;
        }
        let claimed = store
            .claim_session_mission_outbox(
                &workspace_key,
                &worker_id,
                now_ms(),
                LEASE_MS,
                WORKER_BATCH,
            )
            .await;
        match claimed {
            Ok(records) => {
                for record in records {
                    materialize_mission_membership(&store, &mission, &worker_id, record).await;
                }
            }
            Err(error) => {
                tracing::error!(%error, workspace_key, "mission membership outbox claim failed")
            }
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
    }
}

async fn materialize_mission_membership(
    store: &UnifiedSessionStore,
    mission: &runtime::MissionRuntime,
    worker_id: &str,
    record: memory::SessionMissionOutboxRecord,
) {
    let outcome = match record.operation {
        SessionMissionOutboxOperation::Register => mission
            .register_session(runtime::StartMissionSessionRequest {
                title: record.title.clone(),
                session_id: Some(record.session_id.clone()),
            })
            .map(|_| ()),
        SessionMissionOutboxOperation::Start => mission
            .start_session(runtime::StartMissionSessionRequest {
                title: record.title.clone(),
                session_id: Some(record.session_id.clone()),
            })
            .map(|_| ()),
        SessionMissionOutboxOperation::Close => {
            if mission.get_session(&record.session_id).is_some() {
                mission.close_session(&record.session_id).map(|_| ())
            } else {
                // A close may race a never-materialized register. There is no
                // aggregate state to mutate, so the requested final state is
                // already satisfied and must not poison the outbox.
                Ok(())
            }
        }
    };
    match outcome {
        Ok(()) => {
            if let Err(error) = store
                .ack_session_mission_outbox(
                    &record.request_id,
                    worker_id,
                    record.revision,
                    now_ms(),
                )
                .await
            {
                tracing::error!(request_id = %record.request_id, %error, "mission lifecycle applied but outbox acknowledgement failed");
            }
        }
        Err(error) => {
            let retry_at = now_ms().saturating_add(retry_delay_ms(record.attempts));
            if let Err(failure) = store
                .fail_session_mission_outbox(
                    &record.request_id,
                    worker_id,
                    record.revision,
                    OutboxFailureClass::Retryable,
                    &error,
                    retry_at,
                    MAX_ATTEMPTS,
                    now_ms(),
                )
                .await
            {
                tracing::error!(request_id = %record.request_id, error = %failure, "mission lifecycle failure state could not be recorded");
            }
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
    event_store: runtime::SessionTerminalDeliveryPort,
    store: Arc<UnifiedSessionStore>,
    event_bus: Arc<SessionEventBus>,
    mut shutdown: watch::Receiver<bool>,
) {
    let worker_id = format!("gateway-delivery:{}", uuid::Uuid::new_v4());
    loop {
        if *shutdown.borrow() {
            break;
        }
        let claim_store = event_store.clone();
        let claim_worker = worker_id.clone();
        let claimed = tokio::task::spawn_blocking(move || {
            claim_store.claim(&claim_worker, now_ms(), LEASE_MS, WORKER_BATCH)
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
    event_store: &runtime::SessionTerminalDeliveryPort,
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
                    "type": "TerminalCommitted",
                    "session_id": record.session_id,
                    "terminal_id": record.terminal_id,
                    "message_id": record.message_id,
                    "sequence": message.sequence,
                    "response": text,
                    "runtime_commit_cursor": record.commit_cursor,
                });
                event_bus
                    .broadcast(&record.session_id, &event.to_string())
                    .await;
            }
            let event_store = event_store.clone();
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            let acknowledgement = tokio::task::spawn_blocking(move || {
                event_store.acknowledge(&terminal_id, &worker, revision, now_ms())
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
            if let Err(error) = acknowledgement {
                // The durable message ID makes replay safe. Leaving the lease
                // unacked intentionally lets the next worker take it over.
                tracing::error!(terminal_id = %record.terminal_id, %error, "terminal append committed but ack failed");
            }
        }
        Err((class, error)) => {
            let event_store = event_store.clone();
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            let retry_at = now_ms().saturating_add(retry_delay_ms(record.attempts));
            let failure_record = tokio::task::spawn_blocking(move || {
                event_store.fail(
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
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
            if let Err(failure) = failure_record {
                tracing::error!(terminal_id = %record.terminal_id, error = %failure, "terminal failure state could not be recorded");
            }
        }
    }
}

pub(crate) fn decode_terminal_payload(
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
    use memory::{SessionMissionOutboxOperation, SessionMissionOutboxRequest, SessionRecord};
    use tokio::sync::mpsc;

    async fn delivery_fixture() -> (
        runtime::SessionTerminalDeliveryPort,
        Arc<UnifiedSessionStore>,
        Arc<SessionEventBus>,
        mpsc::Receiver<String>,
    ) {
        let event_store = runtime::RuntimeServices::in_memory()
            .unwrap()
            .session_terminal_delivery();
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
    async fn mission_membership_bridge_replays_registration_once() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        let record = SessionRecord {
            session_id: "mission-session".to_string(),
            platform: "test".to_string(),
            chat_id: "mission-session".to_string(),
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
        };
        let request = SessionMissionOutboxRequest {
            request_id: "mission-register-1".to_string(),
            session_id: record.session_id.clone(),
            title: "Mission session".to_string(),
            workspace_key: "workspace-a".to_string(),
            operation: SessionMissionOutboxOperation::Register,
            created_at_ms: 100,
        };
        store
            .upsert_session_with_mission_outbox(&record, &request)
            .await
            .unwrap();
        let claimed = store
            .claim_session_mission_outbox("workspace-a", "worker", 100, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mission = Arc::new(
            runtime::MissionRuntime::event_sourced(
                Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap()),
                "workspace-a",
            )
            .unwrap(),
        );

        materialize_mission_membership(&store, &mission, "worker", claimed).await;

        assert!(mission.get_session("mission-session").is_some());
        assert_eq!(
            store
                .get_session_mission_outbox("mission-register-1")
                .await
                .unwrap()
                .unwrap()
                .status,
            memory::OutboxStatus::Materialized
        );
    }

    #[tokio::test]
    async fn mission_membership_replay_after_lost_ack_is_idempotent() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        let record = SessionRecord {
            session_id: "mission-replay".to_string(),
            platform: "test".to_string(),
            chat_id: "mission-replay".to_string(),
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
        };
        let request = SessionMissionOutboxRequest {
            request_id: "mission-replay-1".to_string(),
            session_id: record.session_id.clone(),
            title: "Replay session".to_string(),
            workspace_key: "workspace-a".to_string(),
            operation: SessionMissionOutboxOperation::Register,
            created_at_ms: 100,
        };
        store
            .upsert_session_with_mission_outbox(&record, &request)
            .await
            .unwrap();
        let mission = Arc::new(
            runtime::MissionRuntime::event_sourced(
                Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap()),
                "workspace-a",
            )
            .unwrap(),
        );
        let first = store
            .claim_session_mission_outbox("workspace-a", "worker-a", 100, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();

        // Runtime applied the event, but the bridge process lost ownership
        // before the acknowledgement. A restarted worker must replay safely.
        materialize_mission_membership(&store, &mission, "wrong-worker", first).await;
        let replay = store
            .claim_session_mission_outbox("workspace-a", "worker-b", 150, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        materialize_mission_membership(&store, &mission, "worker-b", replay).await;

        assert_eq!(mission.list_sessions().len(), 1);
        assert_eq!(
            mission
                .events()
                .iter()
                .filter(|event| event.event_type == "mission.session.registered")
                .count(),
            1
        );
        assert_eq!(
            store
                .get_session_mission_outbox("mission-replay-1")
                .await
                .unwrap()
                .unwrap()
                .status,
            memory::OutboxStatus::Materialized
        );
    }

    #[tokio::test]
    async fn append_success_ack_failure_replays_without_duplicate_message_or_event() {
        let (event_store, store, event_bus, mut rx) = delivery_fixture().await;
        event_store
            .enqueue("t1", "m1", "s1", 7, "assistant_json:\"done\"")
            .unwrap();
        let record = event_store
            .claim("owner-a", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();

        deliver_terminal(&event_store, &store, &event_bus, "wrong-owner", record).await;
        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        let terminal_event: serde_json::Value =
            serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(terminal_event["type"], "TerminalCommitted");
        assert_eq!(terminal_event["terminal_id"], "t1");
        assert_eq!(terminal_event["message_id"], "m1");
        assert_eq!(terminal_event["runtime_commit_cursor"], 7);
        assert_eq!(event_store.get("t1").unwrap().unwrap().status, "claimed");

        let reclaimed = event_store
            .claim("owner-b", 110, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        deliver_terminal(&event_store, &store, &event_bus, "owner-b", reclaimed).await;
        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        assert!(rx.try_recv().is_err(), "replay must not rebroadcast");
        assert_eq!(
            event_store.get("t1").unwrap().unwrap().status,
            "materialized"
        );
    }

    #[tokio::test]
    async fn corrupt_terminal_is_poisoned_and_visible_to_operations() {
        let (event_store, store, event_bus, _rx) = delivery_fixture().await;
        event_store
            .enqueue("poison", "m2", "s1", 8, "not-typed")
            .unwrap();
        let record = event_store
            .claim("worker", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        deliver_terminal(&event_store, &store, &event_bus, "worker", record).await;
        let poison = event_store.blocked(10).unwrap();
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
            .enqueue(
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
                event_store.clone(),
                Arc::clone(&store),
                Arc::clone(&event_bus),
                receiver,
            ));
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if event_store
                        .get("restart-t1")
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
