//! Production projection from canonical Runtime trace events into Growth.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness_contract::growth::GrowthEvent;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
) -> Result<runtime::RuntimeProjectionLane, String> {
    let descriptor = runtime::RuntimeProjectionDescriptor::new(
        GROWTH_PROJECTOR_ID,
        runtime::RuntimeProjectionInterest::new([runtime::RuntimeProjectionEventInterest::new(
            runtime::RuntimeEventScope::Task,
            GROWTH_EVENT_KIND,
        )]),
        GROWTH_BATCH,
        Duration::from_secs(30),
    )?
    .with_latency_class(runtime::RuntimeProjectionLatencyClass::Maintenance);
    Ok(runtime::RuntimeProjectionLane::asynchronous(
        descriptor,
        move |batch_size| {
            let config_home = config_home.clone();
            let event_store = Arc::clone(&event_store);
            let growth = growth.clone();
            let memory = memory.clone();
            let matrix = matrix.clone();
            Box::pin(async move {
                project_growth_page(config_home, event_store, growth, memory, matrix, batch_size)
                    .await
            })
        },
    ))
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
    let prepared = tokio::task::spawn_blocking(move || {
        scan_store.run_projection_work(runtime::RuntimeProjectionWorkClass::Background, || {
            prepare_growth_page(&scan_store, max_commits)
        })
    })
    .await
    .map_err(|error| format!("Growth projection scan worker failed: {error}"))??;
    let Some((checkpoint, page)) = prepared else {
        return Ok(runtime::RuntimeProjectionPass::scanned(
            0,
            max_commits.max(1),
        ));
    };

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
            if event.source_event_kind == "approval.risk_receipt"
                && (source.actor.as_deref() != Some("approval.risk_receipt")
                    || source.stream_id != format!("growth:risk:{}", event.id))
            {
                return Err("Growth risk source producer identity mismatch".into());
            }
            let receipt = growth
                .ingest_admitted_growth_event(&config_home, &memory, &matrix, event)
                .await;
            validate_receipt(&receipt)?;
        }
    }

    // The admitted page stays durable until every original target receipt is
    // settled. A concurrent stop may set paused; preserve it on completion.
    let current = checkpoint_growth(&event_store)?;
    if current.payload["pending_page"] != checkpoint.payload["pending_page"] {
        return Err("Growth admitted page changed before settlement".into());
    }
    let mut payload = current.payload.clone();
    payload
        .as_object_mut()
        .ok_or("invalid Growth checkpoint")?
        .remove("pending_page");
    event_store
        .compare_and_put_projection_checkpoint(
            GROWTH_PROJECTOR_ID,
            page.scanned_through_cursor,
            current.revision,
            &payload,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;

    Ok(
        runtime::RuntimeProjectionPass::scanned(page.scanned_commits, max_commits.max(1))
            .with_matches(page.matched_events),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrowthAdmittedPage {
    from: u64,
    through: u64,
    max_commits: usize,
    scanned_commits: usize,
    digest: String,
}

fn growth_interest() -> runtime::RuntimeProjectionInterest {
    runtime::RuntimeProjectionInterest::new([runtime::RuntimeProjectionEventInterest::new(
        runtime::RuntimeEventScope::Task,
        GROWTH_EVENT_KIND,
    )])
}
fn checkpoint_growth(
    store: &runtime::RuntimeEventStore,
) -> Result<runtime::RuntimeProjectionCheckpoint, String> {
    store
        .projection_checkpoint(GROWTH_PROJECTOR_ID)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Growth policy checkpoint missing".into())
}
/// Upgrade the original cursor in place. No source history is promoted merely
/// by acquiring policy controls; an old fully committed cursor stays committed.
pub(crate) fn initialize_growth_policy(
    store: &runtime::RuntimeEventStore,
    enabled: bool,
) -> Result<(), String> {
    let old = store
        .projection_checkpoint(GROWTH_PROJECTOR_ID)
        .map_err(|e| e.to_string())?;
    if old
        .as_ref()
        .is_some_and(|c| c.payload.get("paused").is_some())
    {
        growth_checkpoint_status(store)?;
        if !enabled {
            pause_growth_sources(store)?;
        }
        return Ok(());
    }
    let cursor = old.as_ref().map_or(0, |c| c.source_cursor);
    store
        .compare_and_put_projection_checkpoint(
            GROWTH_PROJECTOR_ID,
            cursor,
            old.as_ref().map_or(0, |c| c.revision),
            &serde_json::json!({"schema_version":1,"paused":!enabled}),
            now_ms(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}
pub(crate) fn growth_checkpoint_status(store: &runtime::RuntimeEventStore) -> Result<bool, String> {
    let c = checkpoint_growth(store)?;
    c.payload["paused"]
        .as_bool()
        .ok_or("invalid Growth pause state")?;
    if let Some(value) = c.payload.get("pending_page") {
        let p: GrowthAdmittedPage =
            serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        if p.from != c.source_cursor
            || p.through <= p.from
            || p.max_commits == 0
            || p.max_commits > GROWTH_BATCH
            || p.scanned_commits == 0
            || p.scanned_commits > p.max_commits
            || p.through > store.durable_commit_cursor().map_err(|e| e.to_string())?
            || p.digest.len() != 64
        {
            return Err("invalid Growth admitted page identity".into());
        }
        Ok(false)
    } else {
        Ok(true)
    }
}
pub(crate) fn pause_growth_sources(store: &runtime::RuntimeEventStore) -> Result<(), String> {
    // A pass can finish during this synchronous mutation; re-read its revision,
    // never overwrite a newly admitted page with an earlier payload.
    for _ in 0..3 {
        let c = checkpoint_growth(store)?;
        if c.payload["paused"].as_bool() == Some(true) {
            return Ok(());
        }
        let mut payload = c.payload.clone();
        payload["paused"] = true.into();
        payload["pause_cut"] = store
            .durable_commit_cursor()
            .map_err(|e| e.to_string())?
            .into();
        match store.compare_and_put_projection_checkpoint(
            GROWTH_PROJECTOR_ID,
            c.source_cursor,
            c.revision,
            &payload,
            now_ms(),
        ) {
            Ok(_) => return Ok(()),
            Err(runtime::RuntimeEventStoreError::StaleRevision { .. }) => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
    Err("Growth pause raced an original checkpoint writer".into())
}
pub(crate) fn resume_growth_sources(store: &runtime::RuntimeEventStore) -> Result<(), String> {
    let c = checkpoint_growth(store)?;
    if c.payload["paused"].as_bool() == Some(false) {
        return Ok(());
    }
    if !growth_checkpoint_status(store)? {
        return Err("Growth original admitted page remains pending".into());
    }
    let cut = store.durable_commit_cursor().map_err(|e| e.to_string())?;
    let mut payload = c.payload.clone();
    payload["paused"] = false.into();
    payload["skipped_from"] = c.source_cursor.into();
    payload["resume_cut"] = cut.into();
    store
        .compare_and_put_projection_checkpoint(
            GROWTH_PROJECTOR_ID,
            cut,
            c.revision,
            &payload,
            now_ms(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}
fn page_digest(page: &runtime::RuntimeProjectionScanPage) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                page.scanned_through_cursor,
                page.scanned_commits,
                &page.batches
            ))
            .map_err(|e| e.to_string())?
        )
    ))
}
fn prepare_growth_page(
    store: &runtime::RuntimeEventStore,
    max_commits: usize,
) -> Result<
    Option<(
        runtime::RuntimeProjectionCheckpoint,
        runtime::RuntimeProjectionScanPage,
    )>,
    String,
> {
    // Original callers/tests may invoke the lane directly without RuntimeHost.
    if store
        .projection_checkpoint(GROWTH_PROJECTOR_ID)
        .map_err(|e| e.to_string())?
        .is_none()
    {
        initialize_growth_policy(store, true)?;
    }
    let mut c = checkpoint_growth(store)?;
    c.payload["paused"]
        .as_bool()
        .ok_or("invalid Growth pause state")?;
    let admitted = c.payload.get("pending_page").cloned();
    if admitted.is_none() && c.payload["paused"].as_bool() == Some(true) {
        return Ok(None);
    }
    let plan: Option<GrowthAdmittedPage> = admitted
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| e.to_string())?;
    if plan.is_some() {
        growth_checkpoint_status(store)?;
    }
    let limit = plan
        .as_ref()
        .map_or(max_commits.clamp(1, GROWTH_BATCH), |p| p.max_commits);
    let mut page = store
        .projection_scan_page(
            c.source_cursor,
            &growth_interest(),
            limit,
            GROWTH_BATCH,
            32 * 1024 * 1024,
        )
        .map_err(|e| e.to_string())?;
    if let Some(p) = plan {
        page.batches.retain(|b| b.commit_cursor <= p.through);
        page.scanned_through_cursor = p.through;
        page.scanned_commits = p.scanned_commits;
        page.matched_events = page.batches.iter().map(|b| b.events.len()).sum();
        if page_digest(&page)? != p.digest {
            return Err("Growth admitted source page digest changed".into());
        }
    } else {
        if page.scanned_through_cursor <= c.source_cursor {
            return Ok(None);
        }
        let p = GrowthAdmittedPage {
            from: c.source_cursor,
            through: page.scanned_through_cursor,
            max_commits: limit,
            scanned_commits: page.scanned_commits,
            digest: page_digest(&page)?,
        };
        let mut payload = c.payload.clone();
        payload["pending_page"] = serde_json::to_value(p).map_err(|e| e.to_string())?;
        c = store
            .compare_and_put_projection_checkpoint(
                GROWTH_PROJECTOR_ID,
                c.source_cursor,
                c.revision,
                &payload,
                now_ms(),
            )
            .map_err(|e| e.to_string())?;
    }
    Ok(Some((c, page)))
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
        || !matches!(
            envelope.growth_event.source_event_kind.as_str(),
            GROWTH_EVENT_KIND | "approval.risk_receipt"
        )
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
        fail_memory_receipt: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FactLedger for SlowFactLedger {
        fn catalog_page(
            &self,
            query: &fact_kernel::FactCatalogQuery,
        ) -> FactLedgerResult<fact_kernel::FactCatalogPage> {
            self.inner.catalog_page(query)
        }
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
            if record.target == "memory.entry"
                && record.error.is_none()
                && record.status == "promoted"
                && self.fail_memory_receipt.swap(false, Ordering::SeqCst)
            {
                return Err(fact_kernel::FactLedgerError::backend(
                    "injected original memory receipt failure",
                ));
            }
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

    fn append_source(store: &runtime::RuntimeEventStore, id: &str) -> u64 {
        store.append(runtime::RuntimeEventInput{stream_id:format!("growth-source:{id}"),scope:runtime::RuntimeEventScope::Task,
            kind:GROWTH_EVENT_KIND.into(),status:None,actor:None,refs:Vec::new(),payload:serde_json::json!({"growth_event_schema_version":1,"growth_event":event()})}).unwrap().commit_cursor
    }
    #[test]
    fn growth_pause_preserves_only_original_admission_and_resume_skips_the_paused_interval() {
        let store = runtime::RuntimeEventStore::for_test();
        initialize_growth_policy(&store, true).unwrap();
        let a = append_source(&store, "A");
        let (_, original) = prepare_growth_page(&store, 8).unwrap().unwrap();
        assert_eq!(original.batches.len(), 1);
        assert_eq!(original.scanned_through_cursor, a);
        pause_growth_sources(&store).unwrap();
        let b = append_source(&store, "B");
        assert!(!growth_checkpoint_status(&store).unwrap());
        assert!(resume_growth_sources(&store)
            .unwrap_err()
            .contains("pending"));
        let (c, recovered) = prepare_growth_page(&store, 8).unwrap().unwrap();
        assert_eq!(
            recovered, original,
            "pending source cut must not expand to B"
        );
        // This test isolates checkpoint CAS; actual effects/receipts are checked
        // by the PG partial-effect test, not fabricated as successful here.
        let mut payload = c.payload.clone();
        payload.as_object_mut().unwrap().remove("pending_page");
        store
            .compare_and_put_projection_checkpoint(
                GROWTH_PROJECTOR_ID,
                a,
                c.revision,
                &payload,
                now_ms(),
            )
            .unwrap();
        assert!(prepare_growth_page(&store, 8).unwrap().is_none());
        resume_growth_sources(&store).unwrap();
        assert_eq!(checkpoint_growth(&store).unwrap().source_cursor, b);
        let c = append_source(&store, "C");
        let (_, next) = prepare_growth_page(&store, 8).unwrap().unwrap();
        assert_eq!(next.scanned_through_cursor, c);
        assert_eq!(next.batches.len(), 1);
        assert_eq!(next.batches[0].events[0].stream_id, "growth-source:C");
    }
    #[test]
    fn growth_admitted_page_rejects_foreign_cut_digest_and_unknown_pause() {
        for field in ["from", "through", "digest", "paused"] {
            let store = runtime::RuntimeEventStore::for_test();
            initialize_growth_policy(&store, true).unwrap();
            append_source(&store, "original");
            prepare_growth_page(&store, 8).unwrap().unwrap();
            let c = checkpoint_growth(&store).unwrap();
            let mut p = c.payload.clone();
            match field {
                "from" => p["pending_page"][field] = 99.into(),
                "through" => p["pending_page"][field] = 999.into(),
                "digest" => p["pending_page"][field] = "0".repeat(64).into(),
                _ => p[field] = "unknown".into(),
            }
            store
                .compare_and_put_projection_checkpoint(
                    GROWTH_PROJECTOR_ID,
                    c.source_cursor,
                    c.revision,
                    &p,
                    now_ms(),
                )
                .unwrap();
            assert!(
                growth_checkpoint_status(&store).is_err()
                    || prepare_growth_page(&store, 8).is_err(),
                "{field}"
            );
            assert_eq!(checkpoint_growth(&store).unwrap().source_cursor, 0);
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
        let workspace = config_home.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let topology = Arc::new(
            crate::selected_storage::SelectedStorageTopology::compose_for_test(
                &config_home,
                &workspace,
            )
            .expect("isolated PostgreSQL topology"),
        );
        let store = Arc::clone(&topology.runtime_event_store);
        let growth = GrowthService::with_ledger(Arc::clone(&topology.fact_ledger));
        let manager = Arc::new(
            CognitiveContextManager::new_with_selected_store_and_auxiliaries(
                test_memory_config(&config_home.join("storage/memory-index")),
                Some(workspace),
                Arc::clone(&topology.memory_store),
                Some(topology.memory_maintenance_queue.clone()),
            )
            .await
            .unwrap(),
        );
        let memory = MemoryService::with_manager_and_knowledge(
            Some(manager),
            topology.knowledge_fabric.clone(),
        );
        let matrix = MatrixService::with_store(
            Arc::clone(&topology.matrix_store),
            topology
                .registry
                .endpoint(&storage::StorageDomainId::Matrix)
                .expect("matrix endpoint")
                .clone(),
        );
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
                )
                .unwrap()],
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
        let store = Arc::new(runtime::RuntimeEventStore::for_test());
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
                )
                .unwrap()],
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
        let pending = checkpoint_growth(&store).unwrap();
        assert_eq!(pending.source_cursor, 0);
        assert!(pending.payload.get("pending_page").is_some());
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
    }

    #[tokio::test]
    async fn growth_partial_receipt_cold_recovery_preserves_effects_and_skips_paused_sources() {
        async fn selected(
            root: &std::path::Path,
            t: &crate::selected_storage::SelectedStorageTopology,
        ) -> (MemoryService, MatrixService) {
            let manager = Arc::new(
                CognitiveContextManager::new_with_selected_store_and_auxiliaries(
                    test_memory_config(&root.join("storage/memory-index")),
                    Some(root.join("workspace")),
                    Arc::clone(&t.memory_store),
                    Some(t.memory_maintenance_queue.clone()),
                )
                .await
                .unwrap(),
            );
            (
                MemoryService::with_manager_and_knowledge(
                    Some(manager),
                    t.knowledge_fabric.clone(),
                ),
                MatrixService::with_store(
                    Arc::clone(&t.matrix_store),
                    t.registry
                        .endpoint(&storage::StorageDomainId::Matrix)
                        .unwrap()
                        .clone(),
                ),
            )
        }
        fn append(store: &runtime::RuntimeEventStore, id: &str) {
            let mut value = rich_event();
            value.id = id.into();
            store.append(runtime::RuntimeEventInput{stream_id:format!("growth:{id}"),scope:runtime::RuntimeEventScope::Task,
                kind:GROWTH_EVENT_KIND.into(),status:None,actor:None,refs:Vec::new(),
                payload:serde_json::json!({"growth_event_schema_version":1,"growth_event":value})}).unwrap();
        }
        async fn await_settled(store: &runtime::RuntimeEventStore) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !growth_checkpoint_status(store).unwrap() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("original reactor must recover admitted page without test retry");
        }
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("workspace")).unwrap();
        let t = Arc::new(
            crate::selected_storage::SelectedStorageTopology::compose_for_test(
                root.path(),
                &root.path().join("workspace"),
            )
            .unwrap(),
        );
        t.preserve_failed_test_namespace().unwrap();
        let (memory, matrix) = selected(root.path(), &t).await;
        let store = Arc::clone(&t.runtime_event_store);
        let hit = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let growth = GrowthService::with_ledger(Arc::new(SlowFactLedger {
            inner: Arc::clone(&t.fact_ledger),
            delay: Duration::ZERO,
            fail_memory_receipt: hit.clone(),
        }));
        let mut candidate = rich_event();
        candidate.id = "A".into();
        let wrong = super::super::growth_service::memory_candidate_entry_for_test(
            &candidate,
            &candidate.memory_candidates[0],
        );
        let refused = memory
            .remember_entry_with_context(wrong, "session-1", "growth")
            .await
            .unwrap_err();
        assert!(
            refused.contains("source cut is stale or foreign"),
            "{refused}"
        );
        assert_eq!(
            memory
                .list_all_entries()
                .await
                .unwrap()
                .iter()
                .filter(|e| e.source_agent.as_deref() == Some("growth-service"))
                .count(),
            0
        );
        initialize_growth_policy(&store, true).unwrap();
        append(&store, "A");
        let failed = project_growth_page(
            root.path().to_path_buf(),
            store.clone(),
            growth.clone(),
            memory.clone(),
            matrix.clone(),
            8,
        )
        .await
        .unwrap_err();
        assert!(
            failed.contains("injected original memory receipt failure"),
            "{failed}"
        );
        assert!(!hit.load(Ordering::SeqCst));
        assert_eq!(
            memory
                .list_all_entries()
                .await
                .unwrap()
                .iter()
                .filter(|e| e.source_agent.as_deref() == Some("growth-service"))
                .count(),
            1,
            "receipt fault must follow a real successful Memory effect"
        );

        assert!(!growth_checkpoint_status(&store).unwrap());
        assert_eq!(checkpoint_growth(&store).unwrap().source_cursor, 0);
        let matrix_before = t
            .fact_ledger
            .list_growth_promotions()
            .unwrap()
            .into_iter()
            .filter(|p| p.event_id == "A" && p.target == "matrix.fact")
            .collect::<Vec<_>>();
        assert_eq!(matrix_before.len(), 1);
        assert!(matrix_before[0].error.is_none());
        assert!(
            t.fact_ledger.list_facts().unwrap().len() >= 2,
            "eligible Fact effects must precede injected receipt failure"
        );
        pause_growth_sources(&store).unwrap();
        append(&store, "B");
        drop((memory, matrix, growth, store));
        let old_port = Arc::downgrade(&t.runtime_event_store);
        let lease = Arc::try_unwrap(t)
            .unwrap_or_else(|_| panic!("old adapters retained"))
            .detach_for_test_restart()
            .unwrap();
        assert!(old_port.upgrade().is_none());
        let t = Arc::new(lease.rebuild().unwrap());
        let (memory, matrix) = selected(root.path(), &t).await;
        let store = Arc::clone(&t.runtime_event_store);
        let growth = GrowthService::with_ledger(Arc::clone(&t.fact_ledger));
        let lane = || {
            growth_projection_lane(
                root.path().to_path_buf(),
                store.clone(),
                growth.clone(),
                memory.clone(),
                matrix.clone(),
            )
            .unwrap()
        };
        let reactor =
            Arc::new(runtime::RuntimeEventReactor::sealed(store.clone(), [lane()]).unwrap());
        reactor.start().unwrap();
        await_settled(&store).await;
        assert_eq!(
            growth
                .durable_event_log()
                .unwrap()
                .iter()
                .map(|e| e.id.as_str())
                .collect::<Vec<_>>(),
            vec!["A"]
        );
        let matrix_after = t
            .fact_ledger
            .list_growth_promotions()
            .unwrap()
            .into_iter()
            .filter(|p| p.event_id == "A" && p.target == "matrix.fact")
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::to_value(matrix_after).unwrap(),
            serde_json::to_value(matrix_before).unwrap(),
            "completed original Matrix receipt must not be rewritten"
        );
        let entries = memory.list_all_entries().await.unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.source_agent.as_deref() == Some("growth-service"))
                .count(),
            1
        );
        reactor.stop_lane(GROWTH_PROJECTOR_ID);
        reactor
            .drain_lane(
                GROWTH_PROJECTOR_ID,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        resume_growth_sources(&store).unwrap();
        append(&store, "C");
        reactor.register_lane(lane()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while growth.durable_event_log().unwrap().len() != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
        let ids = growth
            .durable_event_log()
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect::<Vec<_>>();
        assert!(
            ids.contains(&"A".into()) && ids.contains(&"C".into()) && !ids.contains(&"B".into())
        );
        let mut foreign = rich_event();
        foreign.id = "A".into();
        foreign.session_id = "foreign-session".into();
        let refused = growth
            .ingest_growth_event(root.path(), &memory, &matrix, foreign)
            .await;
        assert!(!refused.durable);
        assert!(refused.errors.iter().any(|e| e.contains("foreign payload")));
        assert_eq!(growth.durable_event_log().unwrap().len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_fact_projection_does_not_block_tokio_heartbeat() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let topology = crate::selected_storage::SelectedStorageTopology::compose_for_test(
            root.path(),
            &workspace,
        )
        .expect("isolated PostgreSQL topology");
        let growth = GrowthService::with_ledger(Arc::new(SlowFactLedger {
            inner: topology.fact_ledger.clone(),
            delay: Duration::from_millis(150),
            fail_memory_receipt: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
