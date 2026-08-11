//! Production projection from canonical Runtime trace events into Growth.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness_contract::growth::GrowthEvent;
use serde::Deserialize;

use super::{GrowthIngestReceipt, GrowthService, MatrixService, MemoryService};

pub(crate) const GROWTH_PROJECTOR_ID: &str = "projector:growth:v1";
const GROWTH_EVENT_KIND: &str = "runtime.harness_contract.trace";
const GROWTH_EVENT_SCHEMA_VERSION: u32 = 1;
const GROWTH_BATCH: usize = 8;

static GROWTH_DLQ_COUNT: AtomicU64 = AtomicU64::new(0);

/// Dead-lettered Growth sources counted since this process started.
#[must_use]
pub fn growth_dead_lettered() -> u64 {
    GROWTH_DLQ_COUNT.load(Ordering::Relaxed)
}

#[derive(Debug, Deserialize)]
struct GrowthEventEnvelope {
    growth_event_schema_version: u32,
    growth_event: GrowthEvent,
}

pub(crate) fn growth_projection_lane(
    config_home: PathBuf,
    event_store: Arc<runtime::RuntimeEventStore>,
    growth: GrowthService,
    memory: MemoryService,
    matrix: MatrixService,
) -> runtime::RuntimeProjectionLane {
    let descriptor = runtime::RuntimeProjectionDescriptor::new(
        GROWTH_PROJECTOR_ID,
        runtime::RuntimeProjectionInterest::new([runtime::RuntimeProjectionEventInterest::new(
            runtime::RuntimeEventScope::Task,
            GROWTH_EVENT_KIND,
        )]),
        GROWTH_BATCH,
        Duration::from_secs(30),
    )
    .expect("Growth projection descriptor is static and valid")
    .with_latency_class(runtime::RuntimeProjectionLatencyClass::Maintenance);
    runtime::RuntimeProjectionLane::asynchronous(descriptor, move |batch_size| {
        let config_home = config_home.clone();
        let event_store = Arc::clone(&event_store);
        let growth = growth.clone();
        let memory = memory.clone();
        let matrix = matrix.clone();
        Box::pin(async move {
            project_growth_page(config_home, event_store, growth, memory, matrix, batch_size).await
        })
    })
}

async fn project_growth_page(
    config_home: PathBuf,
    event_store: Arc<runtime::RuntimeEventStore>,
    growth: GrowthService,
    memory: MemoryService,
    matrix: MatrixService,
    max_commits: usize,
) -> Result<runtime::RuntimeProjectionPass, String> {
    let scan_store = Arc::clone(&event_store);
    let (checkpoint, page) = tokio::task::spawn_blocking(move || {
        scan_store.run_projection_work(runtime::RuntimeProjectionWorkClass::Background, || {
            let checkpoint = scan_store
                .projection_checkpoint(GROWTH_PROJECTOR_ID)
                .map_err(|error| error.to_string())?;
            let cursor = checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.source_cursor);
            let interest = runtime::RuntimeProjectionInterest::new([
                runtime::RuntimeProjectionEventInterest::new(
                    runtime::RuntimeEventScope::Task,
                    GROWTH_EVENT_KIND,
                ),
            ]);
            let page = scan_store
                .projection_scan_page(
                    cursor,
                    &interest,
                    max_commits.max(1),
                    GROWTH_BATCH,
                    32 * 1024 * 1024,
                )
                .map_err(|error| error.to_string())?;
            Ok::<_, String>((checkpoint, page))
        })
    })
    .await
    .map_err(|error| format!("Growth projection scan worker failed: {error}"))??;

    for batch in &page.batches {
        for source in &batch.events {
            let event = match decode_growth_event(&source.payload) {
                Ok(event) => event,
                Err(error) => {
                    tracing::warn!(
                        event_id = %source.event_id,
                        %error,
                        "Growth source dead-lettered; continuing the projection pass"
                    );
                    record_growth_dlq(&event_store, &source.event_id, batch.commit_cursor, &error)?;
                    continue;
                }
            };
            let receipt = growth
                .ingest_growth_event(&config_home, &memory, &matrix, event)
                .await;
            validate_receipt(&receipt)?;
        }
    }

    let previous_cursor = checkpoint
        .as_ref()
        .map_or(0, |checkpoint| checkpoint.source_cursor);
    if page.scanned_through_cursor > previous_cursor {
        let checkpoint_store = Arc::clone(&event_store);
        let source_cursor = page.scanned_through_cursor;
        let expected_revision = checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.revision);
        let scanned_commits = page.scanned_commits;
        let matched_events = page.matched_events;
        tokio::task::spawn_blocking(move || {
            checkpoint_store.run_projection_work(
                runtime::RuntimeProjectionWorkClass::Background,
                || {
                    checkpoint_store
                        .compare_and_put_projection_checkpoint(
                            GROWTH_PROJECTOR_ID,
                            source_cursor,
                            expected_revision,
                            &serde_json::json!({
                                "schema_version": GROWTH_EVENT_SCHEMA_VERSION,
                                "source_cursor": source_cursor,
                                "scanned_commits": scanned_commits,
                                "matched_events": matched_events,
                            }),
                            now_ms(),
                        )
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                },
            )
        })
        .await
        .map_err(|error| format!("Growth checkpoint worker failed: {error}"))??;
    }

    Ok(
        runtime::RuntimeProjectionPass::scanned(page.scanned_commits, max_commits.max(1))
            .with_matches(page.matched_events),
    )
}

fn decode_growth_event(payload: &serde_json::Value) -> Result<GrowthEvent, String> {
    let envelope = serde_json::from_value::<GrowthEventEnvelope>(payload.clone())
        .map_err(|error| format!("typed GrowthEvent payload is invalid: {error}"))?;
    if envelope.growth_event_schema_version != GROWTH_EVENT_SCHEMA_VERSION {
        return Err(format!(
            "unsupported GrowthEvent schema version {}",
            envelope.growth_event_schema_version
        ));
    }
    if envelope.growth_event.id.trim().is_empty()
        || envelope.growth_event.session_id.trim().is_empty()
        || envelope.growth_event.source_event_kind != GROWTH_EVENT_KIND
    {
        return Err("GrowthEvent identity/source contract is incomplete".to_string());
    }
    Ok(envelope.growth_event)
}

fn record_growth_dlq(
    event_store: &runtime::RuntimeEventStore,
    event_id: &str,
    source_cursor: u64,
    error: &str,
) -> Result<(), String> {
    GROWTH_DLQ_COUNT.fetch_add(1, Ordering::Relaxed);
    let stream_id = format!("dead-letter:{event_id}");
    event_store
        .append(runtime::RuntimeEventInput {
            stream_id: stream_id.clone(),
            scope: runtime::RuntimeEventScope::Task,
            kind: "growth.projection.dead_lettered".to_string(),
            status: Some("dead_lettered".to_string()),
            actor: Some("growth.projection_lane".to_string()),
            refs: vec![runtime::RuntimeEventRef {
                kind: "source_event".to_string(),
                id: event_id.to_string(),
            }],
            payload: serde_json::json!({
                "schema_version": 1,
                "source_cursor": source_cursor,
                "error": error,
            }),
        })
        .map_err(|error| format!("growth dead-letter record failed: {error}"))?;
    Ok(())
}

fn validate_receipt(receipt: &GrowthIngestReceipt) -> Result<(), String> {
    if !receipt.durable {
        return Err(format!(
            "Growth event {} was not durably recorded: {}",
            receipt.event_id,
            receipt.errors.join("; ")
        ));
    }
    if !receipt.errors.is_empty() {
        return Err(format!(
            "Growth event {} has incomplete projections: {}",
            receipt.event_id,
            receipt.errors.join("; ")
        ));
    }
    const TERMINAL_STATUSES: &[&str] = &[
        "promote",
        "hold",
        "reject",
        "promoted",
        "refreshed",
        "duplicate",
        "conflict_held",
        "held",
    ];
    for promotion in &receipt.promotions {
        if let Some(error) = promotion.error.as_deref() {
            return Err(format!(
                "Growth {} projection for {} is retryable: {error}",
                receipt.event_id, promotion.target
            ));
        }
        if !TERMINAL_STATUSES.contains(&promotion.status.as_str()) {
            return Err(format!(
                "Growth {} projection for {} returned unknown status `{}`",
                receipt.event_id, promotion.target, promotion.status
            ));
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use fact_kernel::{
        EvidencePacket, FactGrowthBatch, FactLedger, FactLedgerResult, FactLedgerSnapshot,
        FactRecallQuery, FactRecord, GrowthPromotionRecord,
    };
    use harness_contract::core::ExecutionPattern;
    use harness_contract::growth::{
        GrowthEvidenceRef, GrowthMatrixSignal, GrowthMemoryCandidate, GrowthMemoryCandidateKind,
    };
    use memory::{
        config::{BudgetConfig, StoreConfig},
        CognitiveContextManager, MemoryConfig,
    };

    struct SlowFactLedger {
        inner: Arc<dyn FactLedger>,
        delay: Duration,
    }

    impl FactLedger for SlowFactLedger {
        fn upsert_fact(&self, fact: FactRecord) -> FactLedgerResult<FactRecord> {
            self.inner.upsert_fact(fact)
        }

        fn get_fact(&self, fact_id: &str) -> FactLedgerResult<Option<FactRecord>> {
            self.inner.get_fact(fact_id)
        }

        fn list_facts(&self) -> FactLedgerResult<Vec<FactRecord>> {
            self.inner.list_facts()
        }

        fn recall_facts(&self, query: &FactRecallQuery) -> FactLedgerResult<Vec<FactRecord>> {
            self.inner.recall_facts(query)
        }

        fn upsert_evidence(&self, evidence: EvidencePacket) -> FactLedgerResult<EvidencePacket> {
            self.inner.upsert_evidence(evidence)
        }

        fn get_evidence(&self, evidence_id: &str) -> FactLedgerResult<Option<EvidencePacket>> {
            self.inner.get_evidence(evidence_id)
        }

        fn list_evidence(&self) -> FactLedgerResult<Vec<EvidencePacket>> {
            self.inner.list_evidence()
        }

        fn record_growth_event(&self, event: GrowthEvent) -> FactLedgerResult<()> {
            self.inner.record_growth_event(event)
        }

        fn list_growth_events(&self) -> FactLedgerResult<Vec<GrowthEvent>> {
            self.inner.list_growth_events()
        }

        fn record_growth_promotion(&self, record: GrowthPromotionRecord) -> FactLedgerResult<()> {
            self.inner.record_growth_promotion(record)
        }

        fn list_growth_promotions(&self) -> FactLedgerResult<Vec<GrowthPromotionRecord>> {
            self.inner.list_growth_promotions()
        }

        fn persist_growth_fact_batch(&self, batch: FactGrowthBatch) -> FactLedgerResult<()> {
            std::thread::sleep(self.delay);
            self.inner.persist_growth_fact_batch(batch)
        }

        fn export_snapshot(&self) -> FactLedgerResult<FactLedgerSnapshot> {
            self.inner.export_snapshot()
        }
    }

    fn event() -> GrowthEvent {
        GrowthEvent {
            id: "growth-event-1".to_string(),
            session_id: "session-1".to_string(),
            source_event_kind: GROWTH_EVENT_KIND.to_string(),
            strategy_pattern: ExecutionPattern::Execute,
            learning_record_id: "learning-1".to_string(),
            signals: Vec::new(),
            evidence_refs: Vec::new(),
            memory_candidates: Vec::new(),
            matrix_signals: Vec::new(),
            confidence_bp: 9_000,
        }
    }

    fn rich_event() -> GrowthEvent {
        let mut event = event();
        event.evidence_refs = vec![GrowthEvidenceRef::new(
            "integration_validation",
            "r5:reactor-projection-chain",
            "deterministic Reactor projection validation",
        )];
        event.memory_candidates = vec![GrowthMemoryCandidate {
            id: "growth-memory-r5".to_string(),
            kind: GrowthMemoryCandidateKind::AuthorityPromotion,
            summary: "bounded source replay must retain a transactional receipt".to_string(),
            reason: "R5 recovery evidence passed".to_string(),
            confidence_bp: 9_000,
        }];
        event.matrix_signals = vec![GrowthMatrixSignal {
            fact_type: "ai.validation.r5".to_string(),
            dimensions: serde_json::json!({"phase": "r5", "backend": "dual"}),
            measures: serde_json::json!({"passed": 1}),
            confidence_bp: 9_000,
        }];
        event
    }

    fn test_memory_config(sqlite_path: &std::path::Path) -> MemoryConfig {
        MemoryConfig {
            store: StoreConfig {
                sqlite_path: sqlite_path.to_path_buf(),
                blob_dir: sqlite_path.parent().unwrap().join("blobs"),
                ..Default::default()
            },
            budget: BudgetConfig {
                context_window: 8_000,
                reserved_system: 2_000,
                reserved_response: 1_000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn typed_payload_rejects_unknown_schema_without_legacy_guessing() {
        let payload = serde_json::json!({
            "growth_event_schema_version": 2,
            "growth_event": event(),
        });
        assert!(decode_growth_event(&payload)
            .unwrap_err()
            .contains("unsupported GrowthEvent schema"));
    }

    #[test]
    fn typed_payload_round_trips_the_complete_growth_event() {
        let expected = event();
        let payload = serde_json::json!({
            "growth_event_schema_version": GROWTH_EVENT_SCHEMA_VERSION,
            "growth_event": expected.clone(),
        });
        assert_eq!(decode_growth_event(&payload).unwrap(), expected);
    }

    #[test]
    fn checkpoint_policy_distinguishes_governed_hold_from_infrastructure_failure() {
        let governed = GrowthIngestReceipt {
            event_id: "growth-event-1".to_string(),
            durable: true,
            promotions: vec![super::super::GrowthPromotionReceipt {
                target: "memory.entry".to_string(),
                status: "held".to_string(),
                target_id: None,
                summary: "confidence below policy threshold".to_string(),
                error: None,
            }],
            fact_health_issues: Vec::new(),
            errors: Vec::new(),
        };
        assert!(validate_receipt(&governed).is_ok());
        let mut unavailable = governed;
        unavailable.promotions[0].error = Some("database unavailable".to_string());
        assert!(validate_receipt(&unavailable)
            .unwrap_err()
            .contains("retryable"));
    }

    #[tokio::test]
    async fn reactor_lane_projects_and_checkpoints_a_runtime_growth_event() {
        let root = tempfile::tempdir().unwrap();
        let config_home = root.path().to_path_buf();
        std::fs::create_dir_all(config_home.join("storage")).unwrap();
        let store = Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap());
        let growth = GrowthService::new_for_config_home(&config_home);
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(
                &config_home.join("storage/memory.sqlite"),
            ))
            .await
            .unwrap(),
        );
        let memory = MemoryService::with_manager(Some(manager));
        let matrix = MatrixService::new();
        let event = rich_event();
        store
            .append(runtime::RuntimeEventInput {
                stream_id: "runtime:growth-test".to_string(),
                scope: runtime::RuntimeEventScope::Task,
                kind: GROWTH_EVENT_KIND.to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "growth_event_schema_version": GROWTH_EVENT_SCHEMA_VERSION,
                    "growth_event": event,
                }),
            })
            .unwrap();
        let reactor = Arc::new(
            runtime::RuntimeEventReactor::sealed(
                Arc::clone(&store),
                [growth_projection_lane(
                    config_home.clone(),
                    Arc::clone(&store),
                    growth.clone(),
                    memory.clone(),
                    matrix.clone(),
                )],
            )
            .unwrap(),
        );
        reactor.start().unwrap();
        for _ in 0..100 {
            if store
                .projection_checkpoint(GROWTH_PROJECTOR_ID)
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
        assert!(store
            .projection_checkpoint(GROWTH_PROJECTOR_ID)
            .unwrap()
            .is_some());
        assert_eq!(growth.durable_event_log().unwrap().len(), 1);
        assert!(growth
            .durable_promotion_log()
            .unwrap()
            .iter()
            .any(|item| item.target == "matrix.fact" && item.status == "promoted"));
        assert!(growth
            .recall_facts("transactional receipt", 10)
            .unwrap()
            .iter()
            .any(|item| item.fact.statement.contains("transactional receipt")));
        assert!(memory
            .list_all_entries()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.content.contains("transactional receipt")));
        assert!(matrix
            .list_facts(&config_home, 10)
            .unwrap()
            .iter()
            .any(|fact| fact.fact_type == "ai.validation.r5"));
    }

    #[tokio::test]
    async fn infrastructure_failure_never_advances_growth_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let config_home = root.path().to_path_buf();
        let store = Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap());
        let growth = GrowthService::with_ledger(Arc::new(fact_kernel::UnavailableFactLedger::new(
            "injected Growth ledger outage",
        )));
        store
            .append(runtime::RuntimeEventInput {
                stream_id: "runtime:growth-failure".to_string(),
                scope: runtime::RuntimeEventScope::Task,
                kind: GROWTH_EVENT_KIND.to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "growth_event_schema_version": GROWTH_EVENT_SCHEMA_VERSION,
                    "growth_event": event(),
                }),
            })
            .unwrap();
        let reactor = Arc::new(
            runtime::RuntimeEventReactor::sealed(
                Arc::clone(&store),
                [growth_projection_lane(
                    config_home,
                    Arc::clone(&store),
                    growth,
                    MemoryService::new(),
                    MatrixService::new(),
                )],
            )
            .unwrap(),
        );
        reactor.start().unwrap();
        for _ in 0..100 {
            if reactor
                .lane_health(GROWTH_PROJECTOR_ID)
                .unwrap()
                .is_some_and(|health| health.consecutive_failures > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let health = reactor.lane_health(GROWTH_PROJECTOR_ID).unwrap().unwrap();
        assert!(health.consecutive_failures > 0);
        assert!(health
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("not durably recorded")));
        assert!(store
            .projection_checkpoint(GROWTH_PROJECTOR_ID)
            .unwrap()
            .is_none());
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_fact_projection_does_not_block_tokio_heartbeat() {
        let root = tempfile::tempdir().unwrap();
        let registry = storage::StorageRegistry::default_for_config_home(root.path());
        let fact_endpoint = registry.endpoint(&storage::StorageDomainId::Fact).unwrap();
        let growth_endpoint = registry
            .endpoint(&storage::StorageDomainId::Growth)
            .unwrap();
        let ledger =
            fact_sqlite::SqliteFactLedger::open_with_legacy_growth(fact_endpoint, growth_endpoint)
                .unwrap();
        let growth = GrowthService::with_ledger(Arc::new(SlowFactLedger {
            inner: Arc::new(ledger),
            delay: Duration::from_millis(150),
        }));
        let config_home = root.path().to_path_buf();
        let ingest = tokio::spawn(async move {
            growth
                .ingest_growth_event(
                    config_home,
                    &MemoryService::new(),
                    &MatrixService::new(),
                    event(),
                )
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::timeout(
            Duration::from_millis(50),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .expect("Tokio heartbeat must run while the Fact ledger blocks");
        let receipt = ingest.await.unwrap();
        assert!(receipt.durable);
        assert!(receipt.errors.is_empty());
    }
}
