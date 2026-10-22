use std::sync::{Arc, Barrier};
use std::thread;

use runtime::{ArtifactMetadataRepository, RuntimeServices};
use storage::StaticSecretRefResolver;

use super::*;
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn own865_checkpoint_metadata_does_not_fetch_body_and_tracks_row_replacement() {
    let fixture = PostgresTestScope::new();
    let root = r1_prepared_root("checkpoint-metadata");
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let store = runtime::ArtifactStore::new(&root, repository.clone(),
        runtime::ArtifactStoreConfig { compact_threshold_bytes: 1024 * 1024, ..Default::default() }).unwrap();
    let artifact = store.write_bytes(runtime::ArtifactWriteDescriptor {
        media_type: "application/json".into(), visibility_scope: "session:checkpoint-metadata".into(),
        expected_bytes: Some(242788), original_name: None,
    }, &vec![b'x'; 242788]).await.unwrap();
    let before = repository.body_query_count.load(std::sync::atomic::Ordering::Relaxed);
    let (metadata, revision) = repository.checkpoint_object_metadata(&artifact.sha256).unwrap().unwrap();
    assert_eq!(metadata.bytes, 242788);
    assert!(metadata.compact_body.is_none());
    assert_eq!(repository.body_query_count.load(std::sync::atomic::Ordering::Relaxed), before);
    let body = repository.object(&artifact.sha256).unwrap().unwrap();
    assert_eq!(body.compact_body.unwrap().len(), 242788);
    assert_eq!(repository.body_query_count.load(std::sync::atomic::Ordering::Relaxed), before + 1);
    let executor = fixture.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    connection.execute("UPDATE artifact_objects SET compact_body=compact_body WHERE sha256=$1",
        &[&artifact.sha256]).unwrap();
    let (_, replaced) = repository.checkpoint_object_metadata(&artifact.sha256).unwrap().unwrap();
    assert_ne!(revision, replaced);
}

#[derive(Debug)]
struct PreparedCommitResponseLoss {
    inner: PostgresRuntimeEventStore,
    lose_response: std::sync::atomic::AtomicBool,
    injected: Arc<std::sync::atomic::AtomicBool>,
    checkpoint_fault: Option<&'static str>,
    checkpoint_injected: std::sync::atomic::AtomicBool,
}

macro_rules! forward_prepared_response_loss {
    ($(fn $name:ident(&self $(, $argument:ident: $kind:ty)*) -> $result:ty;)*) => {
        $(fn $name(&self $(, $argument: $kind)*) -> $result {
            self.inner.$name($($argument),*)
        })*
    };
}

impl PreparedCommitResponseLoss {
    fn checkpoint_cas_with_fault(
        &self,
        projection_id: &str,
        write: impl FnOnce() -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let fail = projection_id.starts_with("runtime:prepared-maintenance:")
            && self.checkpoint_fault.is_some()
            && !self
                .checkpoint_injected
                .swap(true, std::sync::atomic::Ordering::SeqCst);
        if fail && self.checkpoint_fault == Some("before") {
            self.injected
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(RuntimeEventStoreError::Corrupt(
                "injected maintenance progress CAS before commit".into(),
            ));
        }
        let checkpoint = write()?;
        if fail {
            self.injected
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(RuntimeEventStoreError::Corrupt(
                "injected committed maintenance progress response loss".into(),
            ));
        }
        Ok(checkpoint)
    }

}

impl RuntimeEventStoreBackend for PreparedCommitResponseLoss {
    fn bounded_event_by_idempotency_key(
        &self, stream: &str, key: &str, through: u64, max_bytes: usize,
    ) -> RuntimeEventStoreResult<runtime::RuntimeBoundedEventLookup> {
        self.inner.bounded_event_by_idempotency_key(stream, key, through, max_bytes)
    }

    fn recent_event_lines(
        &self,
        stream_id: &str,
        references: &[runtime::RuntimeEventRef],
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<runtime::MissionControlEventLine>, String> {
        self.inner
            .recent_event_lines(stream_id, references, through_cursor, max_events, max_bytes)
    }
    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let receipt = self.inner.append_transaction(request)?;
        if self
            .lose_response
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.injected
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(RuntimeEventStoreError::Corrupt(
                "injected committed Graph response loss".into(),
            ));
        }
        Ok(receipt)
    }

    fn source_mutation_fence(&self) -> RuntimeEventStoreResult<runtime::RuntimeSourceMutationFence> {
        self.inner.source_mutation_fence()
    }

    fn compare_and_put_projection_checkpoint_fenced(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &Value,
        updated_at_ms: u64,
        source_fence: &runtime::RuntimeSourceMutationFence,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.checkpoint_cas_with_fault(projection_id, || {
            self.inner.compare_and_put_projection_checkpoint_fenced(
                projection_id, source_cursor, expected_revision, payload, updated_at_ms, source_fence,
            )
        })
    }

    fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.checkpoint_cas_with_fault(projection_id, || {
            self.inner.compare_and_put_projection_checkpoint(
                projection_id, source_cursor, expected_revision, payload, updated_at_ms,
            )
        })
    }

    forward_prepared_response_loss! {
            fn append(&self, input: RuntimeEventInput) -> Result<runtime::DurableRuntimeEvent, String>;
            fn append_transaction_with_terminal(&self, request: AppendTransactionRequest, terminal: SessionTerminalInput) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn consume_verified_decision_lease(&self, lease_id: &str, principal_id: &str, review_id: &str, action: &str, scope: &str, evidence_digest: &str, credential_epoch: u64, consumed_at_ms: u64) -> RuntimeEventStoreResult<()>;
            fn append_transaction_with_verified_decision_lease(&self, request: AppendTransactionRequest, lease: &runtime::VerifiedDecisionLease) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn append_batch_if_revision(&self, stream_id: String, expected_revision: u64, transaction_id: String, events: Vec<RuntimeTransactionEventInput>) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn events_after_cursor(&self, cursor: u64, max_commits: usize) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>>;
            fn projection_scan_page(&self, cursor: u64, interest: &RuntimeProjectionInterest, max_commits: usize, max_events: usize, max_bytes: usize) -> RuntimeEventStoreResult<RuntimeProjectionScanPage>;
            fn projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>>;
            fn projection_checkpoints_with_prefix(&self, prefix: &str) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>>;
            fn put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn compare_and_repair_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool>;
            fn event_by_idempotency_key(&self, stream_id: &str, idempotency_key: &str) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>>;
            fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64>;
            fn list_stream(&self, stream_id: &str) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn count_stream_kinds(&self, stream_id: &str, kinds: &[&str], through_sequence: u64) -> Result<u64, String>;
            fn list_stream_page_desc(&self, stream_id: &str, limit: usize, offset: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn stream_event_count(&self, stream_id: &str) -> Result<usize, String>;
            fn execution_events_for_session(&self, session_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn events_for_root_execution(&self, root_execution_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn events_for_root_execution_kind(&self, root_execution_id: &str, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn events_for_activity(&self, activity_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn list_scope(&self, scope: RuntimeEventScope, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn list_scope_page_asc(&self, scope: RuntimeEventScope, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn list_scope_stream_prefix_page_asc(&self, scope: RuntimeEventScope, stream_prefix: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn list_scope_kind_page_asc(&self, scope: RuntimeEventScope, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn stream_ids_for_scope(&self, scope: RuntimeEventScope) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence_page(&self, scope: RuntimeEventScope, kind: &str, sequence: u64, after: Option<(u64, String)>, limit: usize) -> RuntimeEventStoreResult<Vec<(String, u64)>>;
            fn latest_stream_statuses_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>>;
            fn all_events(&self, limit: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64>;
            fn latest_for_stream(&self, stream_id: &str) -> Result<Option<runtime::DurableRuntimeEvent>, String>;
            fn latest_for_stream_kind(&self, stream_id: &str, kind: &str) -> Result<Option<runtime::DurableRuntimeEvent>, String>;
            fn enqueue_session_terminal(&self, terminal_id: &str, message_id: &str, session_id: &str, commit_cursor: u64, payload_ref: &str) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn claim_session_terminals(&self, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal(&self, terminal_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool>;
            fn session_terminal_for_execution(&self, session_id: &str, execution_id: &str, turn_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn materialized_session_terminals_after(&self, session_id: &str, after_commit_cursor: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth>;
            fn blocked_session_terminals(&self, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn retry_session_terminal(&self, terminal_id: &str, actor: &str, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn adopt_session_terminal_fence(&self, request: &RuntimeSessionTerminalFenceAdoption) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn ack_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn suppress_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn fail_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, class: RuntimeSessionOutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn prepared_content_root(&self, origin: &runtime::PreparedArtifactOrigin, artifact: &runtime::ArtifactRef) -> runtime::RuntimeEventStoreResult<runtime::PreparedContentRoot>;
            fn transaction_stream_revision_exact(&self, transaction_id: &str, stream_id: &str) -> runtime::RuntimeEventStoreResult<Option<runtime::CommittedStreamRevision>>;
            fn list_stream_after(&self, stream_id: &str, after_sequence: u64, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn list_stream_before(&self, stream_id: &str, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
            fn position_page(&self, scope: RuntimeEventScope, identity: &str, kind: Option<&str>, after: Option<(u64,u32)>, through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<runtime::DurableRuntimeEvent>, String>;
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and explicit private plan fixture root"]
fn r1_content_gc_postgres_graph_committed_response_loss_preserves_private_body() {
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
    use runtime::ArtifactMetadataRepository;
    let fixture = PostgresTestScope::new();
    let root = r1_prepared_root("pg-response-loss");
    let config = runtime::ArtifactStoreConfig {
        orphan_grace_ms: 0,
        ..runtime::ArtifactStoreConfig::default()
    };
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let artifacts =
        Arc::new(runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap());
    let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backend = PreparedCommitResponseLoss {
        inner: PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap(),
        lose_response: std::sync::atomic::AtomicBool::new(true),
        injected: injected.clone(),
        checkpoint_fault: None,
        checkpoint_injected: std::sync::atomic::AtomicBool::new(false),
    };
    let events = Arc::new(RuntimeEventStore::from_backend(Arc::new(backend)));
    events.bind_artifact_store(artifacts.clone()).unwrap();
    let graph =
        ExecutionGraph::new("real committed Graph whose caller lost its receipt ".repeat(2048))
            .with_lineage(ExecutionGraphLineage {
                session_id: "prepared-pg".into(),
                turn_id: "lost-response-turn".into(),
                root_task_id: "lost-response-task".into(),
                task_id: "lost-response-task".into(),
                generation: 1,
            });
    let commits = runtime::ExecutionCommitService::new(events.clone());
    assert!(commits.register_graph(graph.clone()).is_err());
    assert!(injected.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(events.stream_revision(&graph.id).unwrap(), 1);
    let event = events.latest_for_stream(&graph.id).unwrap().unwrap();
    let reference: runtime::ArtifactRef =
        serde_json::from_value(event.payload["body"]["artifact"].clone()).unwrap();
    assert!(artifacts.resolve(&reference.selector).is_err());
    let view = events.quiesced_content_roots();
    drop(commits);
    drop(events);
    let mut artifacts = Arc::try_unwrap(artifacts).ok().unwrap();
    let report = view
        .reconcile_prepared_batch(
            &mut artifacts,
            true,
            "pg:response-loss",
            "response-loss-quiesced",
        )
        .unwrap();
    assert_eq!(report.referenced_canonical, 1);
    assert_eq!(report.retired_records, 0);
    assert_eq!(report.blocked, 0);
    assert_eq!(
        artifacts
            .reconcile_quiesced_orphans(true, 100)
            .unwrap()
            .removed_objects,
        0
    );
    assert_eq!(
        repository
            .record(reference.selector.strip_prefix("artifact://").unwrap())
            .unwrap()
            .unwrap()
            .record_class,
        runtime::ArtifactRecordClass::PrivatePrepared
    );
    drop(view);
    drop(artifacts);
    drop(repository);
    let artifacts = Arc::new(
        runtime::ArtifactStore::new(
            &root,
            Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap()),
            config,
        )
        .unwrap(),
    );
    let events = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .unwrap()
            .into_runtime_event_store(),
    );
    events.bind_artifact_store(artifacts).unwrap();
    let recovered = runtime::ExecutionGraphStateStore::new(events)
        .load(&graph.id)
        .unwrap();
    assert_eq!(recovered.objective, graph.objective);
    assert_eq!(recovered.revision, 1);
}

#[derive(Debug)]
struct PreparedReservationResponseLoss {
    inner: PostgresArtifactRepository,
    phase: &'static str,
    token: Arc<std::sync::Mutex<Option<String>>>,
    injected: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl runtime::ArtifactMetadataRepository for PreparedReservationResponseLoss {
    fn reserve_attempt(
        &self,
        object: &runtime::ArtifactObjectRecord,
        quota: u64,
        token: &str,
        publication_id: &str,
    ) -> Result<runtime::ArtifactObjectRecord, String> {
        let object = self
            .inner
            .reserve_attempt(object, quota, token, publication_id)?;
        *self.token.lock().unwrap() = Some(token.into());
        if matches!(self.phase, "reserve" | "release") {
            self.injected.lock().unwrap().push("reserve");
            return Err("injected committed reservation response loss".into());
        }
        Ok(object)
    }
    fn publish_attempt(
        &self,
        record: &runtime::ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
    ) -> Result<(), String> {
        self.inner.publish_attempt(record, generation, token, pin)?;
        if self.phase == "publish" {
            self.injected.lock().unwrap().push("publish");
            return Err("injected committed publication response loss".into());
        }
        Ok(())
    }
    fn release_attempt(&self, token: &str, sha256: &str, generation: &str) -> Result<bool, String> {
        let released = self.inner.release_attempt(token, sha256, generation)?;
        if self.phase == "release" {
            self.injected.lock().unwrap().push("release");
            return Err("injected committed release response loss".into());
        }
        Ok(released)
    }
    forward_prepared_response_loss! {
        fn reservation(&self, token: &str) -> Result<Option<runtime::ArtifactReservation>,String>;
        fn active_attempts_page(&self, after: Option<&str>, limit: usize) -> Result<Vec<runtime::ArtifactReservation>,String>;
        fn prepared_high_water(&self) -> Result<Option<String>,String>;
        fn prepared_page(&self, after: Option<&str>, through: &str, limit: usize) -> Result<runtime::PreparedArtifactPage,String>;
        fn retire_prepared(&self, candidate: &runtime::PreparedArtifactCandidate) -> Result<runtime::PreparedArtifactRetirement,String>;
        fn adopt_live(&self, expected: &runtime::ArtifactRecord) -> Result<(),String>;
        fn prepared_readable(&self, expected: &runtime::ArtifactRecord, origin: &runtime::PreparedArtifactOrigin) -> Result<bool,String>;
        fn claim_object_deletion(&self, sha256: &str, generation: &str) -> Result<bool,String>;
        fn finish_object_deletion(&self, sha256: &str, generation: &str) -> Result<(),String>;
        fn object(&self, sha256: &str) -> Result<Option<runtime::ArtifactObjectRecord>,String>;
        fn checkpoint_object_metadata(&self, sha256: &str) -> Result<Option<(runtime::ArtifactObjectRecord, String)>,String>;
        fn record(&self, artifact_id: &str) -> Result<Option<runtime::ArtifactRecord>,String>;
        fn catalog_page(&self, query: runtime::ArtifactCatalogQuery) -> Result<runtime::ArtifactCatalogPage,String>;
        fn touch(&self, artifact_id: &str, at_ms: u64) -> Result<(),String>;
        fn remove_record(&self, artifact_id: &str) -> Result<(),String>;
        fn unreferenced_objects_before(&self, before_ms: u64, limit: usize) -> Result<Vec<runtime::ArtifactObjectRecord>,String>;
        fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(),String>;
        fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(),String>;
        fn is_pinned(&self, artifact_id: &str, at_ms: u64) -> Result<bool,String>;
        fn stats(&self, at_ms: u64) -> Result<runtime::ArtifactStoreStats,String>;
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and explicit private plan fixture root"]
fn r1_content_gc_postgres_real_prepare_reconciles_reserve_publish_release_response_loss() {
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
    use runtime::ArtifactMetadataRepository;
    for phase in ["reserve", "publish", "release"] {
        let fixture = PostgresTestScope::new();
        let root = r1_prepared_root(phase);
        let token = Arc::new(std::sync::Mutex::new(None));
        let injected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let repository = Arc::new(PreparedReservationResponseLoss {
            inner: PostgresArtifactRepository::new(fixture.reconnect()).unwrap(),
            phase,
            token: token.clone(),
            injected: injected.clone(),
        });
        let config = runtime::ArtifactStoreConfig {
            compact_threshold_bytes: 8,
            orphan_grace_ms: 0,
            ..runtime::ArtifactStoreConfig::default()
        };
        let artifacts =
            Arc::new(runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap());
        let events = Arc::new(
            PostgresRuntimeEventStore::new(fixture.reconnect())
                .unwrap()
                .into_runtime_event_store(),
        );
        events.bind_artifact_store(artifacts.clone()).unwrap();
        let graph = ExecutionGraph::new(format!("actual Graph prepare {phase} ").repeat(2048))
            .with_lineage(ExecutionGraphLineage {
                session_id: "prepared-pg".into(),
                turn_id: "reservation-turn".into(),
                root_task_id: "reservation-task".into(),
                task_id: "reservation-task".into(),
                generation: 1,
            });
        let commits = runtime::ExecutionCommitService::new(events.clone());
        assert!(commits.register_graph(graph.clone()).is_err());
        assert_eq!(events.stream_revision(&graph.id).unwrap(), 0);
        let token = token.lock().unwrap().clone().unwrap();
        let attempt = repository.reservation(&token).unwrap().unwrap();
        assert_eq!(
            attempt.state,
            if phase == "publish" {
                runtime::ArtifactReservationState::Consumed
            } else {
                runtime::ArtifactReservationState::Released
            }
        );
        assert!(injected.lock().unwrap().contains(&phase));
        let view = events.quiesced_content_roots();
        drop(commits);
        drop(events);
        let mut artifacts = Arc::try_unwrap(artifacts).ok().unwrap();
        let report = view
            .reconcile_prepared_batch(&mut artifacts, true, phase, "response-loss-run")
            .unwrap();
        assert_eq!(report.blocked, 0);
        assert_eq!(
            report.retired_records,
            if phase == "publish" { 1 } else { 0 }
        );
        assert_eq!(
            artifacts
                .reconcile_quiesced_orphans(true, 100)
                .unwrap()
                .removed_objects,
            1
        );
        assert!(repository.object(&attempt.sha256).unwrap().is_none());
        assert_eq!(repository.reservation(&token).unwrap().unwrap(), attempt);
        let reopened = PostgresArtifactRepository::new(fixture.reconnect()).unwrap();
        assert_eq!(reopened.reservation(&token).unwrap().unwrap(), attempt);
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and explicit private plan fixture root"]
fn r1_content_gc_prepared_progress_cas_before_and_after_commit_reopens_idempotently() {
    for phase in ["before", "after"] {
        let fixture = PostgresTestScope::new();
        let root = r1_prepared_root("progress-cas");
        let config = runtime::ArtifactStoreConfig {
            orphan_grace_ms: 0,
            ..runtime::ArtifactStoreConfig::default()
        };
        let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
        let artifacts =
            Arc::new(runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap());
        let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let events = Arc::new(RuntimeEventStore::from_backend(Arc::new(
            PreparedCommitResponseLoss {
                inner: PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap(),
                lose_response: std::sync::atomic::AtomicBool::new(false),
                injected: injected.clone(),
                checkpoint_fault: Some(phase),
                checkpoint_injected: std::sync::atomic::AtomicBool::new(false),
            },
        )));
        events.bind_artifact_store(artifacts.clone()).unwrap();
        let goals = runtime::execution_core::GoalStore::new(events.clone());
        goals.create(r1_goal_for_prepared("progress-goal")).unwrap();
        let observation = r1_observation_for_prepared(
            "progress-goal",
            "prepared candidate discarded by the stopped publisher ".repeat(2048),
        );
        let prepared = goals
            .observation_event(&observation, "discarded-progress-candidate".into())
            .unwrap();
        let artifact: runtime::ArtifactRef = serde_json::from_value(
            prepared.event.payload["observation_body"]["body"]["artifact"].clone(),
        )
        .unwrap();
        let view = events.quiesced_content_roots();
        drop(goals);
        drop(events);
        let mut artifacts = Arc::try_unwrap(artifacts).ok().unwrap();
        assert!(view
            .reconcile_prepared_batch(&mut artifacts, true, "pg:progress", "progress-run")
            .is_err());
        assert!(injected.load(std::sync::atomic::Ordering::SeqCst));
        assert!(repository
            .record(artifact.selector.strip_prefix("artifact://").unwrap())
            .unwrap()
            .is_none());
        assert!(repository.object(&artifact.sha256).unwrap().is_some());
        drop(view);
        drop(artifacts);
        drop(repository);
        let events = PostgresRuntimeEventStore::new(fixture.reconnect())
            .unwrap()
            .into_runtime_event_store();
        let view = events.quiesced_content_roots();
        drop(events);
        let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
        let mut artifacts = runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap();
        let report = view
            .reconcile_prepared_batch(&mut artifacts, true, "pg:progress", "progress-run")
            .unwrap();
        assert!(report.inventory_pass_complete);
        assert_eq!(report.retired_records, 0);
        assert_eq!(
            artifacts
                .reconcile_quiesced_orphans(true, 100)
                .unwrap()
                .removed_objects,
            1
        );
        assert!(repository.object(&artifact.sha256).unwrap().is_none());
    }
}

fn r1_prepared_root(label: &str) -> std::path::PathBuf {
    let root = std::path::PathBuf::from(
        std::env::var_os("COWD_R1_06_FIXTURE_ROOT").expect("explicit private fixture root"),
    )
    .join(format!("{label}-{}", uuid::Uuid::new_v4()));
    assert!(
        root.is_absolute()
            && root.starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel")
    );
    root
}

fn r1_terminal_fixture_content(
    store: &RuntimeEventStore,
    fixture: &PostgresTestScope,
    request: &mut AppendTransactionRequest,
    terminal: &mut SessionTerminalInput,
) -> Arc<runtime::ArtifactStore> {
    let root = r1_prepared_root("terminal-body");
    std::fs::create_dir_all(&root).unwrap();
    let source = root.join("terminal-source");
    std::fs::write(&source,serde_json::to_vec(&serde_json::json!({
        "schema_version":runtime::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION,"text":"real persisted terminal body"
    })).unwrap()).unwrap();
    let artifacts = Arc::new(
        runtime::ArtifactStore::new(
            root.join("artifacts"),
            Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap()),
            runtime::ArtifactStoreConfig::default(),
        )
        .unwrap(),
    );
    store.bind_artifact_store(artifacts.clone()).unwrap();
    let artifact = artifacts
        .write_path_blocking(
            runtime::ArtifactWriteDescriptor {
                media_type: "application/vnd.cowd.session-terminal+json".into(),
                visibility_scope: format!("session:{}", terminal.session_id),
                expected_bytes: None,
                original_name: None,
            },
            &source,
        )
        .unwrap();
    artifacts
        .pin(
            &artifact,
            &terminal.terminal_id,
            runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        )
        .unwrap();
    terminal.payload_ref = runtime::encode_session_terminal_artifact_ref(&artifact).unwrap();
    let event = request
        .events
        .iter_mut()
        .find(|input| input.event.kind == "runtime.session.terminal_requested")
        .unwrap();
    event.event.payload = serde_json::to_value(terminal).unwrap();
    artifacts
}

fn r1_goal_for_prepared(identity: &str) -> harness_contract::goal::GoalContract {
    serde_json::from_value(serde_json::json!({
        "id":identity,"session_id":"prepared-pg","objective":"real Goal source CAS",
        "criteria":[{"id":"checked","statement":"verified","status":"open"}],"phase":"execution","scope":"internal",
        "spec_revision":1,"spec_digest":"current","completion":"open","revision":1,"user_sequence":1
    })).unwrap()
}

fn r1_observation_for_prepared(
    identity: &str,
    summary: String,
) -> harness_contract::goal::RuntimeObservation {
    serde_json::from_value(serde_json::json!({
        "identity":{"workspace_id":"workspace","session_id":"prepared-pg","graph_id":"prepared-graph","goal_id":identity,"node_id":"prepared-node"},
        "kind":"tool_progress","source":"prepared-test","source_revision":1,"freshness":{"observed_at_ms":1,"policy_revision":"current"},
        "summary":summary,"fingerprint":identity,"result_class":"succeeded"
    })).unwrap()
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and explicit private plan fixture root"]
fn r1_content_gc_prepared_inventory_resumes_after_retained_prefix() {
    use runtime::ArtifactMetadataRepository;
    let fixture = PostgresTestScope::new();
    let root = r1_prepared_root("prepared-inventory");
    let config = runtime::ArtifactStoreConfig {
        orphan_grace_ms: 0,
        ..runtime::ArtifactStoreConfig::default()
    };
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let artifacts =
        Arc::new(runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap());
    let events = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .unwrap()
            .into_runtime_event_store(),
    );
    events.bind_artifact_store(artifacts.clone()).unwrap();
    let goals = runtime::execution_core::GoalStore::new(events.clone());
    let mut candidates = Vec::new();
    for index in 0..105 {
        let identity = format!("prepared-inventory-{index}");
        goals.create(r1_goal_for_prepared(&identity)).unwrap();
        let observation = r1_observation_for_prepared(
            &identity,
            format!("large immutable Goal body {index} ").repeat(1024),
        );
        let input = goals
            .observation_event(&observation, format!("candidate-{index}"))
            .unwrap();
        let reference: harness_contract::context::ArtifactRef = serde_json::from_value(
            input.event.payload["observation_body"]["body"]["artifact"].clone(),
        )
        .unwrap();
        candidates.push((reference.selector.clone(), input, observation));
    }
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    for (_, input, _) in &candidates[..100] {
        events
            .append_batch_if_revision(
                input.event.stream_id.clone(),
                1,
                format!("commit:{}", input.event.stream_id),
                vec![input.clone()],
            )
            .unwrap();
    }
    for (_, input, observation) in &candidates[100..] {
        let mut interleaving = observation.clone();
        interleaving.summary = "committed interleaving".into();
        interleaving.fingerprint = "interleaving".into();
        interleaving.source_revision = 2;
        goals.record_observation(interleaving).unwrap();
        assert!(matches!(
            events.append_batch_if_revision(
                input.event.stream_id.clone(),
                1,
                format!("failed:{}", input.event.stream_id),
                vec![input.clone()]
            ),
            Err(RuntimeEventStoreError::StaleRevision {
                expected: 1,
                actual: 2,
                ..
            })
        ));
    }
    let high = repository.prepared_high_water().unwrap().unwrap();
    let page = repository.prepared_page(None, &high, 100).unwrap();
    assert_eq!(page.candidates.len(), 100);
    assert!(!page.complete);
    let view = events.quiesced_content_roots();
    drop(goals);
    drop(events);
    let mut artifacts = Arc::try_unwrap(artifacts).ok().unwrap();
    let clone = artifacts.clone();
    assert!(view
        .reconcile_prepared_batch(&mut artifacts, true, "pg:inventory", "retained-prefix-run")
        .is_err());
    assert_eq!(
        repository
            .prepared_page(None, &high, 100)
            .unwrap()
            .candidates
            .len(),
        100
    );
    drop(clone);
    let first = view
        .reconcile_prepared_batch(&mut artifacts, true, "pg:inventory", "retained-prefix-run")
        .unwrap();
    assert_eq!(first.examined, 100);
    assert_eq!(first.referenced_canonical, 100);
    assert_eq!(first.retired_records, 0);
    assert!(!first.inventory_pass_complete);
    drop(view);
    drop(artifacts);
    drop(repository);
    let reopened_events = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    let reopened_view = reopened_events.quiesced_content_roots();
    drop(reopened_events);
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let mut artifacts = runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap();
    let second = reopened_view
        .reconcile_prepared_batch(&mut artifacts, true, "pg:inventory", "retained-prefix-run")
        .unwrap();
    assert_eq!(second.examined, 5);
    assert_eq!(second.retired_records, 5);
    assert_eq!(second.blocked, 0);
    assert!(second.inventory_pass_complete);
    assert_eq!(
        artifacts
            .reconcile_quiesced_orphans(true, 100)
            .unwrap()
            .removed_objects,
        5
    );
    for (index, (selector, _, _)) in candidates.iter().enumerate() {
        assert_eq!(
            repository
                .record(selector.strip_prefix("artifact://").unwrap())
                .unwrap()
                .is_some(),
            index < 100
        );
    }
    let third = reopened_view
        .reconcile_prepared_batch(&mut artifacts, true, "pg:inventory", "retained-prefix-run")
        .unwrap();
    assert!(third.inventory_pass_complete);
    assert_eq!(third.retired_records, 0);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and explicit private plan fixture root"]
fn r1_content_gc_reservation_attempts_release_only_exact_generation() {
    use runtime::ArtifactMetadataRepository;
    let fixture = PostgresTestScope::new();
    let root = r1_prepared_root("attempt-generation");
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let config = runtime::ArtifactStoreConfig {
        orphan_grace_ms: 0,
        ..runtime::ArtifactStoreConfig::default()
    };
    let mut artifacts = runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap();
    let object = runtime::ArtifactObjectRecord {
        sha256: format!("sha256:{}", "b".repeat(64)),
        bytes: 8,
        tier: runtime::ArtifactObjectTier::Blob,
        compact_body: None,
        created_at_ms: 0,
        generation: "reserved-generation".into(),
    };
    let first = repository
        .reserve_attempt(
            &object,
            config.total_quota_bytes,
            "attempt-first",
            "first-publication",
        )
        .unwrap();
    assert_eq!(
        repository
            .reserve_attempt(
                &object,
                config.total_quota_bytes,
                "attempt-first",
                "first-publication"
            )
            .unwrap(),
        first
    );
    repository
        .reserve_attempt(
            &object,
            config.total_quota_bytes,
            "attempt-second",
            "second-publication",
        )
        .unwrap();
    assert!(repository
        .release_attempt("attempt-first", &object.sha256, "stale")
        .is_err());
    assert!(repository
        .release_attempt("attempt-first", &object.sha256, &first.generation)
        .unwrap());
    assert!(!repository
        .release_attempt("attempt-first", &object.sha256, &first.generation)
        .unwrap());
    assert!(!repository
        .claim_object_deletion(&object.sha256, &first.generation)
        .unwrap());
    let record = runtime::ArtifactRecord {
        record_class: runtime::ArtifactRecordClass::Live,
        prepared_origin: None,
        artifact_id: "second-publication".into(),
        sha256: object.sha256.clone(),
        bytes: object.bytes,
        media_type: "application/octet-stream".into(),
        visibility_scope: "session:attempt".into(),
        tier: object.tier.clone(),
        created_at_ms: 0,
        last_access_at_ms: 0,
    };
    assert!(repository
        .publish_attempt(&record, &first.generation, "attempt-first", None)
        .is_err());
    repository
        .publish_attempt(&record, &first.generation, "attempt-second", None)
        .unwrap();
    repository
        .publish_attempt(&record, &first.generation, "attempt-second", None)
        .unwrap();
    assert!(!repository
        .release_attempt("attempt-second", &object.sha256, &first.generation)
        .unwrap());
    assert_eq!(
        repository
            .reservation("attempt-second")
            .unwrap()
            .unwrap()
            .state,
        runtime::ArtifactReservationState::Consumed
    );
    repository.remove_record(&record.artifact_id).unwrap();
    assert_eq!(
        artifacts
            .reconcile_quiesced_orphans(true, 100)
            .unwrap()
            .removed_objects,
        1
    );
    let mut replacement = object.clone();
    replacement.generation = "replacement".into();
    repository
        .reserve_attempt(
            &replacement,
            config.total_quota_bytes,
            "attempt-replacement",
            "replacement-publication",
        )
        .unwrap();
    assert!(!repository
        .release_attempt("attempt-first", &object.sha256, &first.generation)
        .unwrap());
    assert!(!repository
        .claim_object_deletion(&object.sha256, &first.generation)
        .unwrap());
    repository
        .finish_object_deletion(&object.sha256, &first.generation)
        .unwrap();
    assert_eq!(
        repository
            .object(&object.sha256)
            .unwrap()
            .unwrap()
            .generation,
        "replacement"
    );
    let events = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    let view = events.quiesced_content_roots();
    drop(events);
    let report = view
        .reconcile_prepared_batch(&mut artifacts, true, "pg:attempts", "attempts-quiesced")
        .unwrap();
    assert_eq!(report.released_attempts, 1);
    assert!(report.attempt_pass_complete);
    assert_eq!(
        artifacts
            .reconcile_quiesced_orphans(true, 100)
            .unwrap()
            .removed_objects,
        1
    );
    assert!(repository.object(&object.sha256).unwrap().is_none());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and explicit private plan fixture root"]
fn r1_content_gc_postgres_graph_goal_business_source_cas_and_reopen() {
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionNodeStatus,
    };
    use runtime::ArtifactMetadataRepository;
    let fixture = PostgresTestScope::new();
    let root = r1_prepared_root("pg-graph-goal");
    let config = runtime::ArtifactStoreConfig {
        orphan_grace_ms: 0,
        ..runtime::ArtifactStoreConfig::default()
    };
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let artifacts =
        Arc::new(runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap());
    let events = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .unwrap()
            .into_runtime_event_store(),
    );
    events.bind_artifact_store(artifacts.clone()).unwrap();
    let commits = runtime::ExecutionCommitService::new(events.clone());
    let mut graph = ExecutionGraph::new("actual PG Graph recovery content ".repeat(2048))
        .with_lineage(ExecutionGraphLineage {
            session_id: "prepared-pg".into(),
            turn_id: "prepared-turn".into(),
            root_task_id: "prepared-task".into(),
            task_id: "prepared-task".into(),
            generation: 1,
        });
    let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
    let node_id = node.id.clone();
    graph.nodes.push(node);
    let graph = commits.register_graph(graph).unwrap().graph;
    let original = events.latest_for_stream(&graph.id).unwrap().unwrap();
    let canonical: runtime::ArtifactRef =
        serde_json::from_value(original.payload["body"]["artifact"].clone()).unwrap();
    let goals = runtime::execution_core::GoalStore::new(events.clone());
    goals.create(r1_goal_for_prepared("pg-graph-cas")).unwrap();
    let pending = r1_observation_for_prepared(
        "pg-graph-cas",
        "actual large failed Goal producer ".repeat(2048),
    );
    let prepared = goals
        .observation_event(&pending, "failed-graph-cas".into())
        .unwrap();
    let orphan: runtime::ArtifactRef = serde_json::from_value(
        prepared.event.payload["observation_body"]["body"]["artifact"].clone(),
    )
    .unwrap();
    let mut interleaving = pending.clone();
    interleaving.summary = "committed interleaving".into();
    interleaving.source_revision = 2;
    interleaving.fingerprint = "interleaving".into();
    goals.record_observation(interleaving).unwrap();
    let error = commits
        .transition_node(
            &graph,
            &node_id,
            ExecutionNodeStatus::Ready,
            None,
            vec![prepared],
        )
        .err()
        .expect("the real Goal source CAS must reject the prepared transition");
    assert!(
        matches!(error,runtime::execution_core::graph::ExecutionCommitError::EventStore(RuntimeEventStoreError::StaleRevision { ref stream_id, expected:1,actual:2 }) if stream_id=="goal:pg-graph-cas")
    );
    assert_eq!(events.stream_revision(&graph.id).unwrap(), 1);
    assert!(events
        .event_by_idempotency_key("goal:pg-graph-cas", "failed-graph-cas")
        .unwrap()
        .is_none());
    let view = events.quiesced_content_roots();
    drop(goals);
    drop(commits);
    drop(events);
    let mut artifacts = Arc::try_unwrap(artifacts).ok().unwrap();
    let report = view
        .reconcile_prepared_batch(&mut artifacts, true, "pg:graph-goal", "pg-graph-goal-run")
        .unwrap();
    assert_eq!(report.blocked, 0);
    assert!(report.retired_records >= 1);
    assert!(report.referenced_canonical >= 1);
    assert!(repository
        .record(orphan.selector.strip_prefix("artifact://").unwrap())
        .unwrap()
        .is_none());
    assert!(repository
        .record(canonical.selector.strip_prefix("artifact://").unwrap())
        .unwrap()
        .is_some());
    assert!(
        artifacts
            .reconcile_quiesced_orphans(true, 100)
            .unwrap()
            .removed_objects
            >= 1
    );
    drop(view);
    drop(artifacts);
    drop(repository);
    let artifacts = Arc::new(
        runtime::ArtifactStore::new(
            &root,
            Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap()),
            config,
        )
        .unwrap(),
    );
    let events = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .unwrap()
            .into_runtime_event_store(),
    );
    events.bind_artifact_store(artifacts).unwrap();
    let recovered = runtime::ExecutionGraphStateStore::new(events.clone())
        .load(&graph.id)
        .unwrap();
    assert_eq!(recovered.objective, graph.objective);
    assert_eq!(recovered.revision, 1);
    let committed = runtime::ExecutionCommitService::new(events.clone())
        .transition_node(
            &recovered,
            &node_id,
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap();
    assert_eq!(committed.graph.revision, 2);
}

#[test]
fn runtime_event_clean_baseline_has_current_fences_and_discovery_index() {
    assert_eq!(RUNTIME_EVENT_MIGRATIONS.len(), 1);
    assert_eq!(
        RUNTIME_EVENT_MIGRATIONS[0].id,
        "runtime_event.current-baseline"
    );
    let schema = RUNTIME_EVENT_MIGRATIONS[0].statements.join("\n");
    for required in [
        "idx_runtime_session_outbox_execution",
        "(session_id, execution_id, turn_id)",
        "encoded_bytes BIGINT NOT NULL",
        "checked_source_evidence JSONB",
        "input_claim_revision BIGINT",
        "idx_runtime_events_stream_idempotency",
        "runtime_consumed_decision_leases",
    ] {
        assert!(schema.contains(required), "missing {required}");
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_terminal_discovery_requires_unique_exact_execution_source() {
    let (store, fixture) = open_real_store();
    let mut request = AppendTransactionRequest {
        transaction_id: "discovery-tx".into(),
        expected_streams: vec![ExpectedStreamRevision {
            stream_id: "discovery-input".into(),
            expected_revision: 0,
        }],
        events: vec![RuntimeTransactionEventInput {
            event: input(
                "discovery-input",
                RuntimeEventScope::SessionInput,
                "runtime.session.terminal_requested",
            ),
            idempotency_key: Some("discovery-terminal".into()),
            schema_version: 1,
        }],
    };
    let mut terminal = SessionTerminalInput {
        terminal_id: "discovery-terminal".into(),
        message_id: "discovery-message".into(),
        session_id: "session-a".into(),
        execution_id: Some("execution-a".into()),
        turn_id: Some("turn-a".into()),
        request_id: Some("request-a".into()),
        session_generation: Some(1),
        input_sequence: Some(1),
        input_claim_owner: Some("producer".into()),
        input_claim_token: Some("claim".into()),
        input_claim_revision: Some(1),
        controlled_recovery_claim_fingerprints: Vec::new(),
        payload_ref: "payload-a".into(),
    };
    let _artifacts = r1_terminal_fixture_content(&store, &fixture, &mut request, &mut terminal);
    let receipt = store
        .append_transaction_with_terminal(request, terminal)
        .unwrap();
    let expected = store
        .session_terminal("discovery-terminal")
        .unwrap()
        .unwrap();
    assert_eq!(expected.status, "pending");
    assert!(expected.materialized_at_ms.is_none());
    let executor = fixture.reconnect();
    {
        let mut connection = executor.checkout_critical().unwrap();
        connection
            .execute(
                "UPDATE runtime_events SET refs='{}'::JSONB WHERE stream_id='discovery-input'",
                &[],
            )
            .unwrap();
        let future: i64 = connection.query_one("INSERT INTO runtime_commits(transaction_id,request_hash,created_at_ms) VALUES('future-unrelated','future',999999) RETURNING commit_cursor", &[]).unwrap().get(0);
        assert!(future > receipt.commit_cursor as i64);
        connection.execute("INSERT INTO runtime_session_outbox(terminal_id,message_id,session_id,commit_cursor,payload_ref,execution_id,turn_id,status,attempts) SELECT 'unrelated-'||position,'message-'||position,'session-a',$1,'unrelated','execution-other','turn-a','pending',-1 FROM generate_series(1,2048) position", &[&future]).unwrap();
        let index: String = connection.query_one("SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() AND indexname='idx_runtime_session_outbox_execution'", &[]).unwrap().get(0);
        assert!(index.contains("(session_id, execution_id, turn_id)"));
        let mut transaction = connection.transaction().unwrap();
        transaction
            .batch_execute("SET LOCAL enable_seqscan=off")
            .unwrap();
        let plan = transaction.query("EXPLAIN SELECT terminal_id FROM runtime_session_outbox WHERE session_id=$1 AND execution_id=$2 AND turn_id=$3 LIMIT 2", &[&"session-a", &"execution-a", &"turn-a"]).unwrap().iter().map(|row| row.get::<_,String>(0)).collect::<Vec<_>>().join("\n");
        assert!(plan.contains("idx_runtime_session_outbox_execution"));
        transaction.rollback().unwrap();
    }
    assert!(store.list_stream("discovery-input").is_err());
    assert_eq!(
        store
            .session_terminal_for_execution("session-a", "execution-a", "turn-a")
            .unwrap(),
        Some(expected.clone())
    );
    for tuple in [
        ("session-b", "execution-a", "turn-a"),
        ("session-a", "execution-b", "turn-a"),
        ("session-a", "execution-a", "turn-b"),
    ] {
        assert!(store
            .session_terminal_for_execution(tuple.0, tuple.1, tuple.2)
            .unwrap()
            .is_none());
    }
    for tuple in [
        ("", "execution-a", "turn-a"),
        ("session-a", " ", "turn-a"),
        ("session-a", "execution-a", "turn-a\0"),
    ] {
        assert!(matches!(
            store.session_terminal_for_execution(tuple.0, tuple.1, tuple.2),
            Err(RuntimeEventStoreError::InvalidTransaction(_))
        ));
    }
    drop(store);
    let reopened = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    assert_eq!(
        reopened
            .session_terminal_for_execution("session-a", "execution-a", "turn-a")
            .unwrap(),
        Some(expected)
    );
    executor.checkout_critical().unwrap().execute("INSERT INTO runtime_session_outbox(terminal_id,message_id,session_id,commit_cursor,payload_ref,execution_id,turn_id,status) VALUES('conflicting','conflicting','session-a',$1,'conflicting','execution-a','turn-a','pending')", &[&(receipt.commit_cursor as i64)]).unwrap();
    assert!(matches!(
        reopened.session_terminal_for_execution("session-a", "execution-a", "turn-a"),
        Err(RuntimeEventStoreError::Corrupt(_))
    ));
    assert!(reopened
        .session_terminal_for_execution("missing", "execution-a", "turn-a")
        .unwrap()
        .is_none());
}

fn input(stream_id: &str, scope: RuntimeEventScope, kind: &str) -> RuntimeEventInput {
    RuntimeEventInput {
        stream_id: stream_id.to_string(),
        scope,
        kind: kind.to_string(),
        status: Some("running".to_string()),
        actor: Some("runtime-postgres-test".to_string()),
        refs: Vec::new(),
        payload: serde_json::json!({"kind": kind}),
    }
}

pub(super) fn open_real_store() -> (RuntimeEventStore, PostgresTestScope) {
    let scope = PostgresTestScope::new();
    let store = PostgresRuntimeEventStore::new(scope.reconnect())
        .expect("owned Runtime event namespace")
        .into_runtime_event_store();
    (store, scope)
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_bounded_history_mission_metadata_precedes_payload_and_limits_driver_bytes() {
    for cohort in [100usize, 100_000] {
        let fixture = PostgresTestScope::new();
        let backend = Arc::new(PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap());
        let store = RuntimeEventStore::from_backend(backend.clone());
        let references = vec![
            runtime::RuntimeEventRef {
                kind: "mission".into(),
                id: "selected".into(),
            },
            runtime::RuntimeEventRef {
                kind: "task".into(),
                id: "selected-task".into(),
            },
        ];
        let old = store
            .append(RuntimeEventInput {
                stream_id: "mission:selected".into(),
                scope: RuntimeEventScope::Session,
                kind: "session.old_large_payload".into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({"old_payload":"x".repeat(2*1024*1024)}),
            })
            .unwrap();
        let old_lines = store
            .recent_event_lines(
                "mission:selected",
                &references,
                old.commit_cursor,
                50,
                1024 * 1024,
            )
            .unwrap();
        assert_eq!(old_lines.len(), 1);
        assert_eq!(old_lines[0].event_id, old.event_id);
        assert!(store
            .recent_event_lines("mission:empty", &[], 0, 50, 1024 * 1024)
            .unwrap()
            .is_empty());
        let mut oracle = std::collections::VecDeque::new();
        for first in (0..cohort).step_by(100) {
            let mut events = Vec::new();
            for sequence in first..(first + 100).min(cohort) {
                let selected = sequence % 3 != 0;
                events.push(RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: format!("recent-cohort:{sequence}"),
                        scope: RuntimeEventScope::Session,
                        kind: if sequence % 2 == 0 {
                            "session.failed"
                        } else {
                            "session.progress"
                        }
                        .into(),
                        status: Some(
                            if sequence % 2 == 0 {
                                "failed"
                            } else {
                                "running"
                            }
                            .into(),
                        ),
                        actor: Some(format!("actor-{sequence}")),
                        refs: if selected {
                            references.clone()
                        } else {
                            vec![runtime::RuntimeEventRef {
                                kind: "session".into(),
                                id: "ambiguous-foreign".into(),
                            }]
                        },
                        payload: serde_json::json!({"sequence":sequence}),
                    },
                    schema_version: 1,
                    idempotency_key: Some(format!("recent-cohort-{sequence}")),
                });
            }
            let expected_streams = events
                .iter()
                .map(|event| ExpectedStreamRevision {
                    stream_id: event.event.stream_id.clone(),
                    expected_revision: 0,
                })
                .collect();
            let receipt = store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!("recent-cohort-{first}"),
                    expected_streams,
                    events,
                })
                .unwrap();
            let batches = store
                .events_after_cursor(receipt.commit_cursor - 1, 1)
                .unwrap();
            assert_eq!(batches.len(), 1);
            for event in &batches[0].events {
                if !event
                    .refs
                    .iter()
                    .any(|reference| references.contains(reference))
                {
                    continue;
                }
                oracle.push_front(runtime::MissionControlEventLine {
                    event_id: event.event_id.clone(),
                    stream_id: event.stream_id.clone(),
                    cursor: event.commit_cursor,
                    transaction_index: event.transaction_index,
                    scope: "session".into(),
                    kind: event.kind.clone(),
                    status: event.status.clone(),
                    actor: event.actor.clone(),
                    created_at_ms: event.created_at_ms,
                });
                oracle.truncate(50);
            }
        }
        let through = store.durable_commit_cursor().unwrap();
        let later = store
            .append(RuntimeEventInput {
                stream_id: "mission:selected".into(),
                scope: RuntimeEventScope::Session,
                kind: "session.after_fixed_h".into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .unwrap();
        backend.take_recent_read_counts();
        backend.interleave_recent_append(RuntimeEventInput {
            stream_id: "mission:selected".into(),
            scope: RuntimeEventScope::Session,
            kind: "session.during_metadata".into(),
            status: None,
            actor: None,
            refs: Vec::new(),
            payload: serde_json::json!({}),
        });
        let actual = store
            .recent_event_lines("mission:selected", &references, through, 50, 1024 * 1024)
            .unwrap();
        assert_eq!(actual, oracle.into_iter().collect::<Vec<_>>());
        assert!(!actual.iter().any(|line| line.event_id == later.event_id));
        assert!(
            !backend.recent_interleave_pending(),
            "metadata interleave fault-hit"
        );
        assert!(store.durable_commit_cursor().unwrap() > later.commit_cursor);
        let counts = backend.take_recent_read_counts();
        assert_eq!((counts.0, counts.1), (50, 50));
        assert!(counts.2 < 1024 * 1024);
        let encoded_bytes = serde_json::to_vec(&actual).unwrap().len();
        let query_refs = serde_json::to_value(&references).unwrap();
        let plan = fixture
            .reconnect()
            .checkout_online_read()
            .unwrap()
            .query_one(
                &format!(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
                    crate::event_store::RECENT_EVENT_LINE_METADATA_SQL
                ),
                &[&"mission:selected", &query_refs, &(through as i64), &50i64],
            )
            .unwrap()
            .get::<_, Value>(0);
        eprintln!("recent scalar cohort={cohort} metadata_rows={} scalar_rows={} driver_column_payload_bytes={} encoded_lines_bytes={encoded_bytes} plan={plan}",counts.0,counts.1,counts.2);
        let selected = store
            .append(RuntimeEventInput {
                stream_id: "mission:selected".into(),
                scope: RuntimeEventScope::Session,
                kind: "session.large_selected_payload".into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({"payload":"z".repeat(2*1024*1024)}),
            })
            .unwrap();
        let changed = fixture
            .reconnect()
            .checkout_critical()
            .unwrap()
            .execute(
                "UPDATE runtime_events SET refs='{}'::JSONB WHERE event_id=$1",
                &[&selected.event_id],
            )
            .unwrap();
        assert_eq!(changed, 1, "selected refs-decode fault-hit");
        backend.take_recent_read_counts();
        assert_eq!(
            store
                .recent_event_lines(
                    "mission:selected",
                    &references,
                    selected.commit_cursor,
                    50,
                    1024 * 1024
                )
                .unwrap()[0]
                .event_id,
            selected.event_id
        );
        assert_eq!(backend.take_recent_read_counts().1, 50);
        for column in ["actor", "event_id"] {
            let text = if column == "actor" {
                "a".repeat(1024 * 1024 + 1)
            } else {
                "e".repeat(180_000)
            };
            let changed = fixture.reconnect().checkout_critical().unwrap().execute(
                &format!("UPDATE runtime_events SET {column}=$1 WHERE commit_cursor=$2 AND transaction_index=$3"),
                &[&text,&(selected.commit_cursor as i64),&i64::from(selected.transaction_index)],
            ).unwrap();
            assert_eq!(changed, 1, "oversized {column} fault-hit");
            backend.take_recent_read_counts();
            let error = store
                .recent_event_lines(
                    "mission:selected",
                    &references,
                    selected.commit_cursor,
                    50,
                    1024 * 1024,
                )
                .unwrap_err();
            assert!(error.starts_with("HistoryBudgetExceeded:"));
            let denied = backend.take_recent_read_counts();
            assert_eq!((denied.0, denied.1, denied.2), (50, 0, 50 * 24));
            fixture.reconnect().checkout_critical().unwrap().execute(
                "UPDATE runtime_events SET actor=NULL,event_id=$1 WHERE commit_cursor=$2 AND transaction_index=$3",
                &[&selected.event_id,&(selected.commit_cursor as i64),&i64::from(selected.transaction_index)],
            ).unwrap();
        }
        for line in &actual {
            assert_eq!(fixture.reconnect().checkout_critical().unwrap().execute(
                "UPDATE runtime_events SET actor=repeat('c',25000) WHERE commit_cursor=$1 AND transaction_index=$2",
                &[&(line.cursor as i64),&i64::from(line.transaction_index)]).unwrap(),1,"cumulative scalar budget fault-hit");
        }
        backend.take_recent_read_counts();
        assert!(store
            .recent_event_lines("mission:selected", &references, through, 50, 1024 * 1024)
            .unwrap_err()
            .starts_with("HistoryBudgetExceeded:"));
        assert_eq!(backend.take_recent_read_counts(), (50, 0, 50 * 24));
        backend.take_recent_read_counts();
        assert!(store
            .recent_event_lines("mission:selected", &references, through, 0, 1)
            .unwrap()
            .is_empty());
        assert!(store
            .recent_event_lines("mission:selected", &references, through, 1, 0)
            .unwrap()
            .is_empty());
        assert!(store
            .recent_event_lines("mission:selected", &references, u64::MAX, 0, 0)
            .is_err());
        assert_eq!(backend.take_recent_read_counts(), (0, 0, 0));
        let mut connection = fixture.reconnect().checkout_critical().unwrap();
        connection
            .batch_execute("ALTER TABLE runtime_events RENAME TO recent_fault_runtime_events")
            .unwrap();
        let error = store
            .recent_event_lines("mission:selected", &references, through, 50, 1024 * 1024)
            .unwrap_err();
        assert!(
            error.contains("recent metadata query failed"),
            "actual metadata SQL fault-hit: {error}"
        );
        connection
            .batch_execute("ALTER TABLE recent_fault_runtime_events RENAME TO runtime_events")
            .unwrap();
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_bounded_history_backend_enforces_bytes_before_hydration() {
    let fixture = PostgresTestScope::new();
    let backend = Arc::new(PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap());
    let store = RuntimeEventStore::from_backend(backend.clone());
    for sequence in 0..3 {
        let mut event = input(
            "metadata-budget",
            RuntimeEventScope::Session,
            "budget.event",
        );
        event.payload = serde_json::json!({"sequence":sequence,"content":"x".repeat(600_000)});
        store.append(event).unwrap();
    }
    let head = store.stream_revision("metadata-budget").unwrap();
    let first = store
        .list_stream_after("metadata-budget", 0, head, 100, 1024 * 1024)
        .unwrap();
    assert_eq!(first.len(), 1);
    let second = store
        .list_stream_after("metadata-budget", first[0].sequence, head, 100, 1024 * 1024)
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].sequence, 2);
    fixture
        .reconnect()
        .checkout_critical()
        .unwrap()
        .execute(
            "UPDATE runtime_events SET refs='{}'::JSONB WHERE stream_id='metadata-budget'",
            &[],
        )
        .unwrap();
    backend.take_sequence_read_counts();
    let error = store
        .list_stream_after("metadata-budget", 0, head, 100, 1)
        .unwrap_err();
    assert!(error.starts_with("HistoryBudgetExceeded:"));
    assert_eq!(backend.take_sequence_read_counts(), (3, 0));
    let error = store
        .position_page(
            RuntimeEventScope::Session,
            "metadata-budget",
            None,
            None,
            store.durable_commit_cursor().unwrap(),
            100,
            1,
        )
        .unwrap_err();
    assert!(error.starts_with("HistoryBudgetExceeded:"));
    assert_eq!(
        store
            .count_stream_kinds("metadata-budget", &["budget.event", "other"], head)
            .unwrap(),
        3
    );
    assert!(store
        .list_stream_after("metadata-budget", 0, head, 0, 1)
        .unwrap()
        .is_empty());
    assert!(store
        .list_stream_before("metadata-budget", u64::MAX, 1, 1)
        .is_err());
    backend.take_sequence_read_counts();
    assert!(store
        .list_stream_after("metadata-budget", 0, head, 1, 0)
        .unwrap()
        .is_empty());
    assert!(store
        .list_stream_before("metadata-budget", head, 0, 1)
        .unwrap()
        .is_empty());
    assert!(store
        .list_stream_before("metadata-budget", head, 1, 0)
        .unwrap()
        .is_empty());
    assert!(store
        .list_stream_after("metadata-budget", head, head, 1, 1024)
        .unwrap()
        .is_empty());
    assert!(store
        .list_stream_after("metadata-budget", u64::MAX, head, 0, 0)
        .is_err());
    assert!(store
        .list_stream_before("metadata-budget", u64::MAX, 0, 0)
        .is_err());
    assert_eq!(backend.take_sequence_read_counts(), (0, 0));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_content_gc_quota_and_stale_generation_are_fenced() {
    use runtime::ArtifactMetadataRepository;
    let fixture = PostgresTestScope::new();
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let gate = Arc::new(Barrier::new(3));
    let attempts = (0..2)
        .map(|index| {
            let repository = repository.clone();
            let gate = gate.clone();
            thread::spawn(move || {
                let object = runtime::ArtifactObjectRecord {
                    sha256: format!(
                        "sha256:{}",
                        if index == 0 {
                            "a".repeat(64)
                        } else {
                            "b".repeat(64)
                        }
                    ),
                    bytes: 8,
                    tier: runtime::ArtifactObjectTier::Compact,
                    compact_body: Some(vec![index; 8]),
                    created_at_ms: 0,
                    generation: format!("generation-{index}"),
                };
                gate.wait();
                let result = repository.reserve_attempt(
                    &object,
                    8,
                    &format!("quota-{}", object.generation),
                    "quota-record",
                );
                (object, result)
            })
        })
        .collect::<Vec<_>>();
    gate.wait();
    let outcomes = attempts
        .into_iter()
        .map(|attempt| attempt.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes.iter().filter(|(_, result)| result.is_ok()).count(),
        1
    );
    let object = &outcomes
        .iter()
        .find(|(_, result)| result.is_ok())
        .unwrap()
        .0;
    assert_eq!(repository.stats(0).unwrap().physical_bytes, 8);
    assert!(!repository
        .claim_object_deletion(&object.sha256, "stale-generation")
        .unwrap());
    assert!(!repository
        .claim_object_deletion(&object.sha256, &object.generation)
        .unwrap());
    let record = runtime::ArtifactRecord {
        record_class: runtime::ArtifactRecordClass::Live,
        prepared_origin: None,
        artifact_id: "quota-record".into(),
        sha256: object.sha256.clone(),
        bytes: object.bytes,
        media_type: "application/octet-stream".into(),
        visibility_scope: "session:quota".into(),
        tier: object.tier.clone(),
        created_at_ms: 0,
        last_access_at_ms: 0,
    };
    assert!(repository
        .publish_attempt(
            &record,
            "stale-generation",
            &format!("quota-{}", object.generation),
            None
        )
        .is_err());
    repository
        .publish_attempt(
            &record,
            &object.generation,
            &format!("quota-{}", object.generation),
            Some(("audit", i64::MAX as u64)),
        )
        .unwrap();
    assert!(repository.remove_record("quota-record").is_err());
    repository.unpin("quota-record", "audit").unwrap();
    repository.remove_record("quota-record").unwrap();
    assert!(repository
        .claim_object_deletion(&object.sha256, &object.generation)
        .unwrap());
    repository
        .finish_object_deletion(&object.sha256, "stale-generation")
        .unwrap();
    assert!(repository.object(&object.sha256).unwrap().is_some());
    repository
        .finish_object_deletion(&object.sha256, &object.generation)
        .unwrap();
    assert!(repository.object(&object.sha256).unwrap().is_none());
    let mut replacement = object.clone();
    replacement.generation = "new-current-generation".into();
    repository
        .reserve_attempt(
            &replacement,
            8,
            "replacement-reservation",
            "replacement-publication",
        )
        .unwrap();
    assert!(!repository
        .claim_object_deletion(&object.sha256, &object.generation)
        .unwrap());
    repository
        .finish_object_deletion(&object.sha256, &object.generation)
        .unwrap();
    assert_eq!(
        repository
            .object(&object.sha256)
            .unwrap()
            .unwrap()
            .generation,
        "new-current-generation"
    );
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and explicit private plan fixture root"]
async fn r1_content_gc_unknown_append_requires_quiesced_reconciliation() {
    use runtime::{ArtifactGcPort, ArtifactMetadataRepository};
    let fixture = PostgresTestScope::new();
    let root = std::path::PathBuf::from(
        std::env::var_os("COWD_R1_06_FIXTURE_ROOT").expect("explicit fixture root"),
    )
    .join("pg-quiesced");
    assert!(
        root.is_absolute()
            && root.starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel")
    );
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let config = runtime::ArtifactStoreConfig {
        orphan_grace_ms: 0,
        ..runtime::ArtifactStoreConfig::default()
    };
    let store = runtime::ArtifactStore::new(&root, repository.clone(), config).unwrap();
    let descriptor = runtime::ArtifactWriteDescriptor {
        media_type: "application/json".into(),
        visibility_scope: "session:unknown-append".into(),
        expected_bytes: None,
        original_name: None,
    };
    let audit = store
        .write_bytes(descriptor.clone(), b"immutable current audit fact")
        .await
        .unwrap();
    store
        .pin_artifact(&audit, "prepared-before-journal", i64::MAX as u64)
        .unwrap();
    let events = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    let mut event = input(
        "session:unknown-append",
        RuntimeEventScope::Session,
        "current.fact",
    );
    event.payload = serde_json::to_value(&audit).unwrap();
    events.append(event).unwrap();
    let orphan = store
        .write_bytes(descriptor, b"known aborted current content")
        .await
        .unwrap();
    store
        .delete_artifact(&orphan, "session:unknown-append")
        .unwrap();
    let unknown = runtime::ArtifactObjectRecord {
        sha256: format!("sha256:{}", "f".repeat(64)),
        bytes: 4,
        tier: runtime::ArtifactObjectTier::Compact,
        compact_body: Some(b"????".to_vec()),
        created_at_ms: 0,
        generation: "unknown-publication-attempt".into(),
    };
    fixture.reconnect().checkout_critical().unwrap().execute(
        "INSERT INTO artifact_objects(sha256,generation,bytes,tier,compact_body,created_at_ms,inflight) VALUES($1,$2,$3,'compact',$4,0,1)",
        &[&unknown.sha256,&unknown.generation,&(unknown.bytes as i64),&unknown.compact_body],
    ).unwrap();
    drop(events);
    drop(store);
    drop(repository);
    let repository = Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap());
    let mut reopened = runtime::ArtifactStore::new(root, repository.clone(), config).unwrap();
    assert!(reopened.reconcile_quiesced_orphans(false, 100).is_err());
    assert_eq!(reopened.collect_garbage(100).unwrap().removed_objects, 0);
    let report = reopened.reconcile_quiesced_orphans(true, 100).unwrap();
    assert_eq!(report.removed_objects, 1);
    assert!(repository.object(&orphan.sha256).unwrap().is_none());
    assert_eq!(repository.object(&unknown.sha256).unwrap(), Some(unknown));
    assert_eq!(
        reopened
            .read(&audit, "session:unknown-append", None)
            .await
            .unwrap(),
        b"immutable current audit fact"
    );
    let events = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    assert_eq!(
        events.list_stream("session:unknown-append").unwrap()[0].payload,
        serde_json::to_value(&audit).unwrap()
    );
    assert_eq!(
        reopened
            .reconcile_quiesced_orphans(true, 100)
            .unwrap()
            .removed_objects,
        0
    );
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_clean_schema_initialization_is_atomic_and_rejects_old_catalog() {
    for (domain, specs, table) in [
        (
            RUNTIME_EVENT_DOMAIN,
            RUNTIME_EVENT_MIGRATIONS,
            "runtime_events",
        ),
        (ARTIFACT_DOMAIN, ARTIFACT_MIGRATIONS, "artifact_objects"),
        (TASK_DOMAIN, TASK_MIGRATIONS, "runtime_tasks"),
    ] {
        assert_eq!(specs.len(), 1);
        let fixture = PostgresTestScope::new();
        let executor = fixture.reconnect();
        executor.checkout_critical().unwrap().batch_execute(&format!("CREATE TABLE {table}(sentinel TEXT NOT NULL); INSERT INTO {table} VALUES('never rewrite old data')")).unwrap();
        assert!(validate_clean_domain_catalog(&executor, domain, specs).is_err());
        let mut connection = executor.checkout_critical().unwrap();
        assert_eq!(
            connection
                .query_one(&format!("SELECT sentinel FROM {table}"), &[])
                .unwrap()
                .get::<_, String>(0),
            "never rewrite old data"
        );
        assert!(!connection
            .query_one(
                "SELECT to_regclass('cowd_schema_migrations') IS NOT NULL",
                &[]
            )
            .unwrap()
            .get::<_, bool>(0));
        connection
            .batch_execute(&format!("DROP TABLE {table}"))
            .unwrap();
        drop(connection);
        validate_clean_domain_catalog(&executor, domain, specs).unwrap();
        executor.apply_migrations(domain, specs).unwrap();
        validate_clean_domain_catalog(&fixture.reconnect(), domain, specs).unwrap();
        let mut connection = executor.checkout_critical().unwrap();
        connection.execute("INSERT INTO cowd_schema_migrations(id,domain,version,description,checksum,applied_at) SELECT $1,domain,version,description,checksum,applied_at FROM cowd_schema_migrations WHERE id=$2", &[&format!("{domain}.old-catalog"),&specs[0].id]).unwrap();
        let before = connection
            .query(
                "SELECT id,checksum FROM cowd_schema_migrations ORDER BY id",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        drop(connection);
        assert!(validate_clean_domain_catalog(&fixture.reconnect(), domain, specs).is_err());
        let after = executor
            .checkout_critical()
            .unwrap()
            .query(
                "SELECT id,checksum FROM cowd_schema_migrations ORDER BY id",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        assert_eq!(before, after);
    }
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    executor.checkout_critical().unwrap().batch_execute("CREATE TABLE runtime_events(sentinel TEXT); INSERT INTO runtime_events VALUES('retain')").unwrap();
    assert!(executor
        .apply_migrations(RUNTIME_EVENT_DOMAIN, RUNTIME_EVENT_MIGRATIONS)
        .is_err());
    let mut connection = executor.checkout_critical().unwrap();
    assert!(!connection
        .query_one("SELECT to_regclass('runtime_commits') IS NOT NULL", &[])
        .unwrap()
        .get::<_, bool>(0));
    assert_eq!(
        connection
            .query_one("SELECT sentinel FROM runtime_events", &[])
            .unwrap()
            .get::<_, String>(0),
        "retain"
    );
    assert!(!connection.query_one("SELECT to_regclass('cowd_schema_migrations') IS NOT NULL OR to_regclass('cowd_schema_catalogs') IS NOT NULL",&[]).unwrap().get::<_,bool>(0));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_task_constructor_rejects_extra_catalog_ids_without_changes() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresTaskStore::new(executor.clone()).unwrap();
    drop(store);
    assert!(PostgresTaskStore::new(fixture.reconnect()).is_ok());
    executor
        .checkout_critical()
        .unwrap()
        .execute(
            r#"INSERT INTO runtime_tasks(task_id,status,created_at_ms,updated_at_ms,record_json)
         VALUES('constructor-sentinel','pending',1,1,'{"sentinel":"retain"}'::JSONB)"#,
            &[],
        )
        .unwrap();
    executor.checkout_critical().unwrap().execute(
        "INSERT INTO cowd_schema_migrations(id,domain,version,checksum,description)
         SELECT 'runtime_task.old',domain,version,checksum,description FROM cowd_schema_migrations WHERE id=$1",
        &[&TASK_MIGRATIONS[0].id],
    ).unwrap();
    let snapshot = |executor: &PostgresExecutor| -> String {
        executor.checkout_online_read().unwrap().query_one(
            "SELECT jsonb_build_object(
                'migrations', (SELECT jsonb_agg(to_jsonb(record) ORDER BY id) FROM cowd_schema_migrations record),
                'catalogs', (SELECT jsonb_agg(to_jsonb(record) ORDER BY domain) FROM cowd_schema_catalogs record),
                'tasks', (SELECT jsonb_agg(to_jsonb(record) ORDER BY task_id) FROM runtime_tasks record),
                'ddl', (SELECT jsonb_agg(jsonb_build_array(oid::TEXT,relname,relkind::TEXT) ORDER BY relname) FROM pg_class WHERE relnamespace=current_schema()::regnamespace)
            )::TEXT", &[],
        ).unwrap().get(0)
    };
    let before = snapshot(&executor);
    assert!(PostgresTaskStore::new(executor.clone()).is_err());
    assert!(PostgresTaskStore::new(fixture.reconnect()).is_err());
    assert_eq!(snapshot(&executor), before);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_task_constructor_requires_all_current_tables() {
    let old = PostgresTestScope::new();
    let old_executor = old.reconnect();
    old_executor.checkout_critical().unwrap().batch_execute(
        "CREATE TABLE runtime_tasks(sentinel TEXT NOT NULL); INSERT INTO runtime_tasks VALUES('retain old data')",
    ).unwrap();
    assert!(PostgresTaskStore::new(old_executor.clone()).is_err());
    let mut connection = old_executor.checkout_online_read().unwrap();
    assert_eq!(
        connection
            .query_one("SELECT sentinel FROM runtime_tasks", &[])
            .unwrap()
            .get::<_, String>(0),
        "retain old data"
    );
    assert!(!connection.query_one("SELECT to_regclass('cowd_schema_migrations') IS NOT NULL OR to_regclass('cowd_schema_catalogs') IS NOT NULL", &[]).unwrap().get::<_, bool>(0));
    drop(connection);

    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    assert!(PostgresTaskStore::new(executor.clone()).is_ok());
    executor
        .checkout_critical()
        .unwrap()
        .batch_execute("DROP TABLE runtime_task_mission_assignment_outbox")
        .unwrap();
    let before: String = executor.checkout_online_read().unwrap().query_one(
        "SELECT jsonb_build_object(
            'migrations', (SELECT jsonb_agg(to_jsonb(record) ORDER BY id) FROM cowd_schema_migrations record),
            'catalogs', (SELECT jsonb_agg(to_jsonb(record) ORDER BY domain) FROM cowd_schema_catalogs record)
        )::TEXT", &[],
    ).unwrap().get(0);
    assert!(PostgresTaskStore::new(fixture.reconnect()).is_err());
    let mut connection = executor.checkout_online_read().unwrap();
    assert!(!connection
        .query_one(
            "SELECT to_regclass('runtime_task_mission_assignment_outbox') IS NOT NULL",
            &[]
        )
        .unwrap()
        .get::<_, bool>(0));
    let after: String = connection.query_one(
        "SELECT jsonb_build_object(
            'migrations', (SELECT jsonb_agg(to_jsonb(record) ORDER BY id) FROM cowd_schema_migrations record),
            'catalogs', (SELECT jsonb_agg(to_jsonb(record) ORDER BY domain) FROM cowd_schema_catalogs record)
        )::TEXT", &[],
    ).unwrap().get(0);
    assert_eq!(before, after);
}

fn policy_bound_task_spec(session_id: &str, objective: &str) -> TaskSpec {
    let policy = SessionExecutionPolicy::from_profile(
        AutonomyProfileId::Supervised,
        1,
        SessionExecutionPolicyOrigin::SessionExplicit,
    );
    let mut spec = TaskSpec::new(objective);
    spec.execution_policy = spec.execution_policy.bind(ExecutionPolicyBinding::bind(
        session_id,
        &policy,
        PermissionMode::ReadOnly,
    ));
    spec
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn projection_work_class_maps_background_without_downgrading_recovery() {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("projection-lanes.pg".to_string(), url)]);
    let pool_set = storage::PostgresPoolSet::connect(
        storage::PostgresPoolSetConfig {
            connection: PostgresConnectionConfig::new(
                "runtime-projection-lanes",
                "projection-lanes.pg",
                "cowd-runtime-projection-lanes",
            ),
            server_reserve: 1,
            critical: storage::PostgresPoolLaneConfig::new(2, Some(1), 1_000),
            online_read: storage::PostgresPoolLaneConfig::new(2, Some(1), 1_000),
            background: storage::PostgresPoolLaneConfig::new(1, Some(1), 250),
        },
        &resolver,
    )
    .expect("isolated pool set");
    let fixture = PostgresTestScope::new();
    let executor = fixture.bind(&pool_set.executor());
    let store = PostgresRuntimeEventStore::new(executor.clone())
        .expect("runtime store")
        .into_runtime_event_store();
    let before = executor.health();
    store.run_projection_work(runtime::RuntimeProjectionWorkClass::Background, || {
        store
            .append(input(
                "projection:background",
                RuntimeEventScope::Evolution,
                "projection.background",
            ))
            .unwrap();
        store.events_after_cursor(0, 1).unwrap();
        store
            .put_projection_checkpoint(
                "projector:lane-proof",
                1,
                &serde_json::json!({"ok": true}),
                1,
            )
            .unwrap();
    });
    let after_background = executor.health();
    let delta = |health: &storage::PostgresExecutorHealth,
                 workload: storage::PostgresWorkloadClass| {
        let current = health
            .lanes
            .iter()
            .find(|lane| lane.workload == workload)
            .unwrap()
            .metrics
            .checkout_count;
        let prior = before
            .lanes
            .iter()
            .find(|lane| lane.workload == workload)
            .unwrap()
            .metrics
            .checkout_count;
        current.saturating_sub(prior)
    };
    assert!(
        delta(
            &after_background,
            storage::PostgresWorkloadClass::Background
        ) >= 3
    );
    assert_eq!(
        delta(&after_background, storage::PostgresWorkloadClass::Critical),
        0
    );
    assert_eq!(
        delta(
            &after_background,
            storage::PostgresWorkloadClass::OnlineRead
        ),
        0
    );

    store.run_projection_work(runtime::RuntimeProjectionWorkClass::Recovery, || {
        store.events_after_cursor(0, 1).unwrap();
        store
            .append(input(
                "projection:recovery",
                RuntimeEventScope::Recovery,
                "projection.recovery",
            ))
            .unwrap();
    });
    let after_recovery = executor.health();
    assert!(
        after_recovery
            .lanes
            .iter()
            .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
            .unwrap()
            .metrics
            .checkout_count
            > after_background
                .lanes
                .iter()
                .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
                .unwrap()
                .metrics
                .checkout_count
    );
    assert!(
        after_recovery
            .lanes
            .iter()
            .find(|lane| lane.workload == storage::PostgresWorkloadClass::Critical)
            .unwrap()
            .metrics
            .checkout_count
            > after_background
                .lanes
                .iter()
                .find(|lane| lane.workload == storage::PostgresWorkloadClass::Critical)
                .unwrap()
                .metrics
                .checkout_count
    );
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_sequence_pages_freeze_the_head_during_independent_concurrent_appends() {
    let (store, fixture) = open_real_store();
    let stream = "program:sequence-page";
    for index in 0..64 {
        let mut event = input(stream, RuntimeEventScope::Program, "page.seed");
        event.payload = serde_json::json!({"seed":index});
        store.append(event).unwrap();
    }
    let frozen_head = store.stream_revision(stream).unwrap();
    assert_eq!(frozen_head, 64);
    let barrier = Arc::new(Barrier::new(3));
    let writers = (0..2)
        .map(|writer| {
            let adapter = PostgresRuntimeEventStore::new(fixture.reconnect())
                .unwrap()
                .into_runtime_event_store();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                (0..32)
                    .map(|index| {
                        let mut event =
                            input(stream, RuntimeEventScope::Program, "page.concurrent");
                        event.payload = serde_json::json!({"writer":writer,"index":index});
                        adapter.append(event).unwrap()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let read_range = |store: &RuntimeEventStore, from: u64, through: u64| {
        let mut cursor = from;
        let mut events = vec![];
        while cursor < through {
            let page = store
                .list_stream_after(stream, cursor, through, 7, 8 * 1024)
                .unwrap();
            assert!(!page.is_empty(), "a stable committed interval has no gaps");
            assert!(page.len() <= 7);
            for event in page {
                assert_eq!(event.sequence, cursor + 1);
                assert!(event.sequence <= through);
                cursor = event.sequence;
                events.push(event);
            }
        }
        events
    };
    barrier.wait();
    let first = read_range(&store, 0, frozen_head);
    let written = writers
        .into_iter()
        .flat_map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();
    let new_head = store.stream_revision(stream).unwrap();
    assert_eq!(new_head, 128);
    assert!(first.iter().all(|event| event.kind == "page.seed"));
    let tail = read_range(&store, frozen_head, new_head);
    let written_ids = written
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let read_ids = tail
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(read_ids, written_ids);
    assert_eq!(read_ids.len(), 64);
    let reopened = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    let replay = read_range(&reopened, 0, new_head);
    let expected = first
        .iter()
        .chain(tail.iter())
        .map(|event| (&event.event_id, &event.payload))
        .collect::<Vec<_>>();
    assert_eq!(
        replay
            .iter()
            .map(|event| (&event.event_id, &event.payload))
            .collect::<Vec<_>>(),
        expected
    );
    assert!(reopened
        .list_stream_after(stream, new_head, new_head, 7, 8 * 1024)
        .unwrap()
        .is_empty());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_runtime_event_store_preserves_fences_outbox_restart_and_runtime_composition() {
    let (store, fixture) = open_real_store();
    store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "postgres-source-transaction".to_string(),
            expected_streams: vec![
                ExpectedStreamRevision {
                    stream_id: "copy:stream".to_string(),
                    expected_revision: 0,
                },
                ExpectedStreamRevision {
                    stream_id: "copy:empty-stream".to_string(),
                    expected_revision: 0,
                },
            ],
            events: vec![input(
                "copy:stream",
                RuntimeEventScope::Recovery,
                "postgres.source_seeded",
            )
            .into()],
        })
        .expect("PostgreSQL source event");
    let store = Arc::new(store);
    store
        .append(input(
            "graph:concurrent",
            RuntimeEventScope::ExecutionGraph,
            "graph.seeded",
        ))
        .expect("seed append");
    store
        .append(input(
            "evolution:signal:prefix-contract",
            RuntimeEventScope::Evolution,
            "evolution.signal.recorded.v1",
        ))
        .expect("prefix target append");
    store
        .append(input(
            "evolution:signal-other",
            RuntimeEventScope::Evolution,
            "evolution.signal.recorded.v1",
        ))
        .expect("prefix neighbour append");
    store
        .append(input(
            "evolution:mission:prefix-contract",
            RuntimeEventScope::Evolution,
            "evolution.mission.created.v1",
        ))
        .expect("different prefix append");
    let prefix_events = store
        .replay_scope_stream_prefix(RuntimeEventScope::Evolution, "evolution:signal:")
        .expect("prefix replay must be independent of database collation");
    assert_eq!(prefix_events.len(), 1);
    assert_eq!(
        prefix_events[0].stream_id,
        "evolution:signal:prefix-contract"
    );

    let barrier = Arc::new(Barrier::new(2));
    let writers = (0..2)
        .map(|writer| {
            let barrier = Arc::clone(&barrier);
            let store = Arc::clone(&store);
            thread::spawn(move || {
                barrier.wait();
                store.append_transaction(AppendTransactionRequest {
                    transaction_id: format!("concurrent-writer-{writer}"),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: "graph:concurrent".to_string(),
                        expected_revision: 1,
                    }],
                    events: vec![RuntimeTransactionEventInput {
                        event: input(
                            "graph:concurrent",
                            RuntimeEventScope::ExecutionGraph,
                            "graph.concurrent",
                        ),
                        idempotency_key: Some(format!("writer-{writer}")),
                        schema_version: 1,
                    }],
                })
            })
        })
        .collect::<Vec<_>>();
    let outcomes = writers
        .into_iter()
        .map(|writer| writer.join().expect("writer thread"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(RuntimeEventStoreError::StaleRevision { .. })))
            .count(),
        1
    );
    assert_eq!(store.stream_revision("graph:concurrent").unwrap(), 2);
    let commit_cursor_before_checkpoint = *store.subscribe_commits().borrow();
    let checkpoint = store
        .put_projection_checkpoint(
            "projector:postgres-contract",
            commit_cursor_before_checkpoint,
            &serde_json::json!({"cursor": commit_cursor_before_checkpoint}),
            100,
        )
        .expect("mutable projection checkpoint");
    assert_eq!(checkpoint.revision, 1);
    assert_eq!(
        *store.subscribe_commits().borrow(),
        commit_cursor_before_checkpoint,
        "mutable projection checkpoints must not emit journal commits"
    );
    assert_eq!(
        store
            .projection_checkpoint("projector:postgres-contract")
            .expect("read checkpoint")
            .expect("checkpoint exists"),
        checkpoint
    );
    assert!(matches!(
        store.put_projection_checkpoint(
            "projector:postgres-contract",
            commit_cursor_before_checkpoint.saturating_sub(1),
            &serde_json::json!({"cursor": "stale"}),
            101,
        ),
        Err(RuntimeEventStoreError::StaleRevision { .. })
    ));

    let mut terminal_request = AppendTransactionRequest {
        transaction_id: "terminal-transaction".to_string(),
        expected_streams: vec![ExpectedStreamRevision {
            stream_id: "session-input:real".to_string(),
            expected_revision: 0,
        }],
        events: vec![RuntimeTransactionEventInput {
            event: input(
                "session-input:real",
                RuntimeEventScope::SessionInput,
                "runtime.session.terminal_requested",
            ),
            idempotency_key: Some("terminal-request".to_string()),
            schema_version: 1,
        }],
    };
    let mut terminal_input = SessionTerminalInput {
        terminal_id: "terminal-real".to_string(),
        message_id: "message-real".to_string(),
        session_id: "session-real".to_string(),
        execution_id: Some("execution-real".to_string()),
        turn_id: Some("turn-real".to_string()),
        request_id: Some("request-real".to_string()),
        session_generation: Some(1),
        input_sequence: Some(0),
        input_claim_owner: Some("worker-real".to_string()),
        input_claim_token: Some("claim-real".to_string()),
        input_claim_revision: Some(3),
        controlled_recovery_claim_fingerprints: Vec::new(),
        payload_ref: "payload-real".to_string(),
    };
    let terminal_artifacts =
        r1_terminal_fixture_content(&store, &fixture, &mut terminal_request, &mut terminal_input);
    let terminal_receipt = store
        .append_transaction_with_terminal(terminal_request.clone(), terminal_input.clone())
        .expect("terminal transaction");
    assert!(terminal_receipt.commit_cursor > 0);

    let discovered = store
        .session_terminal_for_execution("session-real", "execution-real", "turn-real")
        .expect("exact source read")
        .expect("pending original B");
    assert_eq!(
        discovered,
        store.session_terminal("terminal-real").unwrap().unwrap()
    );
    assert_eq!(discovered.status, "pending");
    assert!(discovered.materialized_at_ms.is_none());
    for tuple in [
        ("session-other", "execution-real", "turn-real"),
        ("session-real", "execution-other", "turn-real"),
        ("session-real", "execution-real", "turn-other"),
    ] {
        assert!(store
            .session_terminal_for_execution(tuple.0, tuple.1, tuple.2)
            .unwrap()
            .is_none());
    }
    assert!(matches!(
        store.session_terminal_for_execution("", "execution-real", "turn-real"),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));

    let claim_barrier = Arc::new(Barrier::new(2));
    let workers = (0..2)
        .map(|worker| {
            let barrier = Arc::clone(&claim_barrier);
            let store = Arc::clone(&store);
            thread::spawn(move || {
                barrier.wait();
                store.claim_session_terminals(&format!("worker-{worker}"), 100, 1_000, 1)
            })
        })
        .collect::<Vec<_>>();
    let claims = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker thread").expect("claim"))
        .collect::<Vec<_>>();
    let claimed = claims.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(claimed.len(), 1);
    let claim = &claimed[0];
    assert_eq!(claim.request_id.as_deref(), Some("request-real"));
    assert_eq!(claim.session_generation, Some(1));
    assert_eq!(claim.input_sequence, Some(0));
    assert_eq!(claim.input_claim_owner.as_deref(), Some("worker-real"));
    assert_eq!(claim.input_claim_token.as_deref(), Some("claim-real"));
    assert_eq!(claim.input_claim_revision, Some(3));
    assert_eq!(claim.status, "claimed");
    assert_eq!(claim.attempts, 1);
    assert_eq!(claim.claim_expires_at_ms, Some(1_100));
    drop(store);
    let store = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .expect("postgres event store reopens after delivery crash")
            .into_runtime_event_store(),
    );
    store
        .bind_artifact_store(terminal_artifacts.clone())
        .unwrap();
    assert!(matches!(
        store.adopt_session_terminal_fence(&RuntimeSessionTerminalFenceAdoption {
            terminal_id: claim.terminal_id.clone(),
            expected_terminal_revision: claim.revision,
            request_id: "request-real".to_string(),
            session_id: "session-real".to_string(),
            turn_id: "turn-real".to_string(),
            session_generation: 1,
            input_sequence: 0,
            claim_owner: "session-worker-reclaimed".to_string(),
            claim_token: "session-claim-reclaimed".to_string(),
            claim_revision: 5,
            claim_expires_at_ms: 2_000,
            adopted_at_ms: 1_099,
        }),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));
    let adoption = RuntimeSessionTerminalFenceAdoption {
        terminal_id: claim.terminal_id.clone(),
        expected_terminal_revision: claim.revision,
        request_id: "request-real".to_string(),
        session_id: "session-real".to_string(),
        turn_id: "turn-real".to_string(),
        session_generation: 1,
        input_sequence: 0,
        claim_owner: "session-worker-reclaimed".to_string(),
        claim_token: "session-claim-reclaimed".to_string(),
        claim_revision: 5,
        claim_expires_at_ms: 2_000,
        adopted_at_ms: 1_100,
    };
    let adopted = store
        .adopt_session_terminal_fence(&adoption)
        .expect("expired delivery claim adopts reclaimed Session fence");
    assert_eq!(adopted.status, "pending");
    assert_eq!(adopted.input_claim_revision, Some(5));
    let replay = store
        .append_transaction_with_terminal(terminal_request.clone(), terminal_input.clone())
        .expect("initial terminal transaction replays after fence adoption");
    assert!(replay.duplicate);
    let mut conflicting_initial_fence = terminal_input;
    conflicting_initial_fence.input_claim_token = Some("different-initial-fence".to_string());
    assert!(matches!(
        store.append_transaction_with_terminal(terminal_request, conflicting_initial_fence),
        Err(RuntimeEventStoreError::TransactionConflict { .. })
    ));
    assert_eq!(
        store
            .adopt_session_terminal_fence(&adoption)
            .expect("adoption replay")
            .revision,
        adopted.revision
    );
    let claim = store
        .claim_session_terminals("worker-after-adoption", 1_101, 1_000, 1)
        .expect("claim adopted terminal")
        .remove(0);
    let materialized = store
        .ack_session_terminal(
            &claim.terminal_id,
            claim.claim_owner.as_deref().expect("claim owner"),
            claim.revision,
            1_102,
        )
        .expect("ack claimed terminal");
    assert_eq!(materialized.status, "materialized");
    assert_eq!(
        store
            .materialized_session_terminals_after("session-real", 0, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        store.enqueue_session_terminal(
            "terminal-unfenced",
            "message-unfenced",
            "session-real",
            terminal_receipt.commit_cursor,
            "payload-unfenced",
        ),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));

    let duplicate_request = AppendTransactionRequest {
        transaction_id: "duplicate-transaction".to_string(),
        expected_streams: vec![ExpectedStreamRevision {
            stream_id: "graph:duplicate".to_string(),
            expected_revision: 0,
        }],
        events: vec![RuntimeTransactionEventInput {
            event: input(
                "graph:duplicate",
                RuntimeEventScope::ExecutionGraph,
                "graph.duplicate",
            ),
            idempotency_key: Some("duplicate".to_string()),
            schema_version: 1,
        }],
    };
    assert!(
        !store
            .append_transaction(duplicate_request.clone())
            .expect("first idempotent transaction")
            .duplicate
    );
    assert!(
        store
            .append_transaction(duplicate_request)
            .expect("duplicate transaction")
            .duplicate
    );

    drop(store);
    let reopened = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .expect("postgres event store reopens")
            .into_runtime_event_store(),
    );
    assert_eq!(reopened.stream_revision("graph:concurrent").unwrap(), 2);
    let terminal = reopened
        .session_terminal("terminal-real")
        .unwrap()
        .expect("terminal persists");
    assert_eq!(terminal.status, "materialized");
    assert_eq!(terminal.execution_id.as_deref(), Some("execution-real"));
    assert_eq!(terminal.turn_id.as_deref(), Some("turn-real"));

    let temp = tempfile::tempdir().expect("temporary Runtime host");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace exists");
    let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
        .runtime_event_store(reopened)
        .artifact_store(Arc::new(
            runtime::ArtifactStore::new(
                temp.path().join("artifact-blobs"),
                Arc::new(
                    PostgresArtifactRepository::new(fixture.reconnect())
                        .expect("PG artifact backend"),
                ),
                runtime::ArtifactStoreConfig::default(),
            )
            .expect("PG artifact store"),
        ))
        .task_aggregate_service(Arc::new(
            PostgresTaskStore::new(fixture.reconnect())
                .expect("PG task backend")
                .into_task_service(),
        ))
        .build()
        .expect("RuntimeServices composes PostgreSQL event backend");
    services.publish_session_execution_policy(
        "session:postgres-composed",
        runtime::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Supervised,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
    let mission_id = services.mission_runtime().default_mission_id().to_string();
    services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "postgres-composed".to_string(),
            mission_id,
            kind: TaskKind::Root,
            origin: TaskOrigin::User,
            origin_session_id: "session:postgres-composed".to_string(),
            origin_turn_id: "turn:postgres-composed".to_string(),
            root_task_id: "postgres-composed".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: services
                .task_runtime_port()
                .bind_task_spec(
                    "session:postgres-composed",
                    None,
                    harness_contract::task::TaskSpec::new(
                        "prove canonical Task outbox reaches PostgreSQL event store",
                    ),
                )
                .expect("bind canonical Task policy for PostgreSQL composition"),
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "test_fixture",
                "test://runtime-postgres/composed-task",
            )],
        })
        .expect("canonical Task outbox reaches PostgreSQL event backend");
    assert!(services
        .event_reader()
        .list_stream("task:postgres-composed")
        .expect("read composed event")
        .iter()
        .any(|event| event.kind == "task.created"));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_task_store_preserves_restart_and_per_task_concurrency() {
    let fixture = PostgresTestScope::new();
    let pg_store = PostgresTaskStore::new(fixture.reconnect()).expect("postgres task store opens");
    let executor = pg_store.executor().clone();
    let target = Arc::new(pg_store.into_task_service());
    let source_task = target
        .create(TaskCreateCommand {
            task_id: "task-pg-durable".to_string(),
            mission_id: "mission-pg-durable".to_string(),
            kind: TaskKind::Root,
            origin: TaskOrigin::User,
            origin_session_id: "session-pg-durable".to_string(),
            origin_turn_id: "turn-pg-durable".to_string(),
            root_task_id: "task-pg-durable".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: policy_bound_task_spec("session-pg-durable", "Verify the task control plane"),
            evidence_refs: vec![EvidenceRef::observed(
                "test_fixture",
                "test://runtime-postgres/task-durability",
            )],
        })
        .expect("PostgreSQL task starts")
        .aggregate;
    let phase = target
        .start_phase(
            &source_task.task_id,
            source_task.revision,
            TaskPhaseSpec {
                name: "postgres-verification".to_string(),
                objective: "prove PostgreSQL preserves the task record".to_string(),
                dependency_refs: Vec::new(),
                plan: vec!["persist task state".to_string()],
                acceptance: vec!["restart equality".to_string()],
                test_commands: vec!["real PostgreSQL task test".to_string()],
            },
            Vec::new(),
        )
        .expect("PostgreSQL phase starts")
        .aggregate;
    let phase_id = phase.phases.last().expect("phase exists").phase_id.clone();
    target
        .record_phase_artifact(
            &source_task.task_id,
            phase.revision,
            &phase_id,
            "evidence",
            "durability",
            "PostgreSQL state is canonical",
            Vec::new(),
        )
        .expect("PostgreSQL artifact persists");

    let barrier = Arc::new(Barrier::new(2));
    let workers = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let target = Arc::clone(&target);
            thread::spawn(move || {
                barrier.wait();
                target.create(TaskCreateCommand {
                    task_id: "task-pg-concurrent".to_string(),
                    mission_id: "mission-pg-concurrent".to_string(),
                    kind: TaskKind::Root,
                    origin: TaskOrigin::User,
                    origin_session_id: "session-pg-concurrent".to_string(),
                    origin_turn_id: "turn-pg-concurrent".to_string(),
                    root_task_id: "task-pg-concurrent".to_string(),
                    parent_task_id: None,
                    predecessor_task_id: None,
                    mission_assignment: TaskMissionAssignment::Default,
                    mission_assigned_by: "test".to_string(),
                    spec: policy_bound_task_spec(
                        "session-pg-concurrent",
                        "one governed concurrent task",
                    ),
                    evidence_refs: Vec::new(),
                })
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().expect("task worker joins"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    let receipts = results
        .into_iter()
        .map(|result| result.expect("concurrent create replays canonical receipt"))
        .collect::<Vec<_>>();
    assert_eq!(receipts[0].receipt, receipts[1].receipt);
    assert_eq!(receipts[0].outbox, receipts[1].outbox);
    let concurrent = target
        .list()
        .expect("target task list")
        .into_iter()
        .find(|task| task.task_id == "task-pg-concurrent")
        .expect("one concurrent task persists");
    assert_eq!(concurrent.objective, "one governed concurrent task");
    assert!(target
        .create(TaskCreateCommand {
            task_id: "task-pg-concurrent".to_string(),
            mission_id: "mission-pg-concurrent".to_string(),
            kind: TaskKind::Root,
            origin: TaskOrigin::User,
            origin_session_id: "session-pg-concurrent".to_string(),
            origin_turn_id: "turn-pg-concurrent".to_string(),
            root_task_id: "task-pg-concurrent".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: policy_bound_task_spec("session-pg-concurrent", "a conflicting objective",),
            evidence_refs: Vec::new(),
        })
        .is_err());

    let organization = MissionOrganizationDecision {
        decision_id: "mission-organization:task-pg-concurrent".to_string(),
        workspace_id: "workspace-pg-contract".to_string(),
        root_task_id: "task-pg-concurrent".to_string(),
        affected_task_ids: vec!["task-pg-concurrent".to_string()],
        action: MissionOrganizationAction::KeepDefault,
        target_mission_id: "mission-pg-concurrent".to_string(),
        proposed_objective: None,
        status: MissionOrganizationStatus::Pending,
        reason: "verify immutable organization root".to_string(),
        candidate_count: 0,
        provider_invoked: false,
        provider_model: None,
        provider_input_tokens: 0,
        provider_output_tokens: 0,
        elapsed_ms: 0,
        rejected_reason: None,
        evidence_refs: vec![EvidenceRef::observed(
            "test_fixture",
            "test://runtime-postgres/organization-root",
        )],
        attempt: 0,
        next_attempt_at_ms: 1,
        claim_token: None,
        revision: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    target
        .save_organization_decision(&organization, None)
        .expect("initial PostgreSQL organization decision persists");
    let mut clustered_replay = organization.clone();
    clustered_replay
        .affected_task_ids
        .push("task-pg-durable".to_string());
    let retained = target
        .save_organization_decision(&clustered_replay, None)
        .expect("mutable cluster membership does not break Root idempotency");
    assert_eq!(retained, organization);
    let mut foreign_root = clustered_replay;
    foreign_root.root_task_id = "task-pg-foreign".to_string();
    assert!(target
        .save_organization_decision(&foreign_root, None)
        .is_err());

    let reopened = PostgresTaskStore::new(fixture.reconnect())
        .expect("postgres task store reopens")
        .into_task_service();
    let restored = reopened
        .list()
        .expect("reopened task list")
        .into_iter()
        .find(|task| task.task_id == source_task.task_id)
        .expect("durable task survives reopen");
    assert!(restored
        .phases
        .iter()
        .any(|candidate| candidate.phase_id == phase_id && !candidate.artifacts.is_empty()));
    assert!(executor.health().metrics.checkout_count > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_artifact_repository_preserves_selector_and_scope_contract() {
    let fixture = PostgresTestScope::new();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let executor = fixture.reconnect();
    let repository = Arc::new(
        PostgresArtifactRepository::new(executor).expect("PostgreSQL artifact migrations apply"),
    );
    let root = tempfile::tempdir().expect("artifact blob root");
    let store = runtime::ArtifactStore::new(
        root.path(),
        repository.clone(),
        runtime::ArtifactStoreConfig {
            compact_threshold_bytes: 8,
            max_object_bytes: 2 * 1024 * 1024,
            total_quota_bytes: 4 * 1024 * 1024,
            gc_high_water_bytes: 3 * 1024 * 1024,
            gc_low_water_bytes: 2 * 1024 * 1024,
            orphan_grace_ms: 0,
        },
    )
    .expect("PostgreSQL artifact store composes");
    let scope = format!("session:artifact-{suffix}");
    let artifact = store
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/octet-stream".to_string(),
                visibility_scope: scope.clone(),
                expected_bytes: Some(32),
                original_name: Some("postgres.bin".to_string()),
            },
            &[0x44; 32],
        )
        .await
        .expect("PostgreSQL artifact write");
    assert!(artifact.selector.starts_with("artifact://"));
    assert_eq!(
        store
            .read(&artifact, &scope, Some(4..12))
            .await
            .expect("PostgreSQL artifact range read"),
        vec![0x44; 8]
    );
    assert!(matches!(
        store.read(&artifact, "session:other", None).await,
        Err(runtime::ArtifactError::Unauthorized)
    ));
    let second_root = tempfile::tempdir().expect("second artifact blob root");
    let second_repository: Arc<dyn runtime::ArtifactMetadataRepository> = repository.clone();
    let second_store = runtime::ArtifactStore::new(
        second_root.path(),
        second_repository,
        runtime::ArtifactStoreConfig {
            compact_threshold_bytes: 8,
            max_object_bytes: 2 * 1024 * 1024,
            total_quota_bytes: 4 * 1024 * 1024,
            gc_high_water_bytes: 3 * 1024 * 1024,
            gc_low_water_bytes: 2 * 1024 * 1024,
            orphan_grace_ms: 0,
        },
    )
    .expect("second PostgreSQL artifact store composes");
    let repeated = second_store
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/octet-stream".to_string(),
                visibility_scope: scope.clone(),
                expected_bytes: Some(32),
                original_name: Some("postgres-repeat.bin".to_string()),
            },
            &[0x44; 32],
        )
        .await
        .expect("repeated hash repairs the selected local blob root");
    assert_eq!(
        second_store
            .read(&repeated, &scope, None)
            .await
            .expect("repaired PostgreSQL artifact read"),
        vec![0x44; 32]
    );
    store
        .pin(
            &artifact,
            "postgres-parity",
            runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        )
        .expect("PostgreSQL artifact pin");
    assert!(store.delete(&artifact, &scope).is_err());
    store
        .unpin(&artifact, "postgres-parity")
        .expect("PostgreSQL artifact unpin");
    store
        .delete(&artifact, &scope)
        .expect("PostgreSQL artifact record delete");
    second_store
        .delete(&repeated, &scope)
        .expect("second PostgreSQL artifact record delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_artifact_catalog_excludes_inflight_and_later_receipts() {
    use sha2::Digest;
    let fixture = PostgresTestScope::new();
    let scoped = fixture.reconnect();
    let repository = Arc::new(PostgresArtifactRepository::new(scoped.clone()).unwrap());
    let root = tempfile::tempdir().unwrap();
    let store = runtime::ArtifactStore::new(
        root.path(),
        repository.clone(),
        runtime::ArtifactStoreConfig::default(),
    )
    .unwrap();
    let descriptor = harness_contract::context::ArtifactWriteDescriptor {
        media_type: "text/plain".into(),
        visibility_scope: "session:catalog-pg".into(),
        expected_bytes: None,
        original_name: None,
    };
    let scopes = vec![descriptor.visibility_scope.clone()];
    let mut expected = std::collections::BTreeSet::new();
    let mut sample = None;
    for index in 0..140 {
        let reference = store
            .write_bytes(
                descriptor.clone(),
                format!("catalog source {index}").as_bytes(),
            )
            .await
            .unwrap();
        expected.insert(reference.selector.clone());
        sample = Some(reference);
    }
    let sample = sample.unwrap();
    // This transaction has inserted its row before the first page, but has
    // not committed. Committing later must not inject it into that snapshot.
    let mut pending_connection = scoped.checkout_critical().unwrap();
    let mut pending = pending_connection.transaction().unwrap();
    pending.execute("INSERT INTO artifact_records(artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms)
        SELECT 'art_inflight',sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms FROM artifact_records WHERE artifact_id=$1", &[&sample.selector.strip_prefix("artifact://").unwrap()]).unwrap();
    let mut page = store
        .discover_page(&scopes, "text/plain", None, 7)
        .await
        .unwrap();
    let first_cursor = page.next_cursor.clone().unwrap();
    pending.commit().unwrap();
    drop(pending_connection);
    let mut actual = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages < 30);
        for record in page.records {
            let reference = record.content_reference();
            assert!(actual.insert(reference.selector.clone()));
            let bytes = store.read(&reference, &scopes[0], None).await.unwrap();
            assert_eq!(
                format!("sha256:{:x}", sha2::Sha256::digest(&bytes)),
                reference.sha256
            );
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        store
            .write_bytes(descriptor.clone(), b"own catalog tool receipt")
            .await
            .unwrap();
        page = store
            .discover_page(&scopes, "text/plain", Some(&cursor), 7)
            .await
            .unwrap();
    }
    assert_eq!(actual, expected);
    let fresh = store
        .discover_page(&scopes, "art_inflight", None, 7)
        .await
        .unwrap();
    assert_eq!(fresh.records.len(), 1);
    assert_eq!(
        fresh.records[0].content_reference().selector,
        "artifact://art_inflight"
    );
    store.delete(&sample, &scopes[0]).unwrap();
    assert!(store
        .discover_page(&scopes, "text/plain", Some(&first_cursor), 7)
        .await
        .unwrap_err()
        .to_string()
        .contains("source changed"));
    drop(store);
    drop(repository);
    drop(scoped);
}

mod host_transcript_root_460_pg_tests {
    use super::*;

    fn backend(scope: &PostgresTestScope) -> PostgresRuntimeEventStore {
        let resolver = StaticSecretRefResolver::new([(
            "pg.root.460".into(),
            std::env::var("COWD_TEST_POSTGRES_URL").expect("isolated test PostgreSQL required"),
        )]);
        let mut config =
            PostgresConnectionConfig::new("host-root-460", "pg.root.460", "cowd-host-root-460");
        config.max_connections = 1;
        config.min_idle_connections = Some(1);
        config.checkout_timeout_ms = 1_000;
        let backend = PostgresRuntimeEventStore::new(
            scope.bind(&PostgresExecutor::connect(config, &resolver).unwrap()),
        )
        .unwrap();
        assert_eq!(backend.executor().health().max_connections, 1);
        backend
    }

    fn header(
        digest: &str,
    ) -> (
        RuntimeTransactionEventInput,
        runtime::ArtifactRef,
        runtime::PreparedArtifactOrigin,
    ) {
        let artifact = runtime::ArtifactRef {
            selector: format!("artifact://art_{}", "a".repeat(64)),
            sha256: format!("sha256:{}", "b".repeat(64)),
            bytes: 128,
            media_type: "application/vnd.cowd.host-transcript+json".into(),
            visibility_scope: "session:session-460-pg".into(),
            durability: harness_contract::context::EvidenceDurability::Durable,
        };
        let key = format!("host-transcript:{digest}");
        let binding: harness_contract::projection::RuntimeActivityBinding = serde_json::from_value(serde_json::json!({
            "root_execution_id":"graph-460-pg","session_id":"session-460-pg","turn_id":"turn-460-pg",
            "root_task_id":"task-460-pg","task_id":"task-460-pg",
            "activity_id":"activity:execution:graph-460-pg:node:node-460-pg","node_id":"node-460-pg",
            "parent_activity_id":"activity:execution:graph-460-pg","initiator_activity_id":"activity:execution:graph-460-pg",
            "revision":1,"fence":1,"generation":1,
        })).unwrap();
        let input = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: "graph-460-pg:node:node-460-pg".into(), scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.host.transcript".into(), status: Some("prepared".into()), actor: Some("tool_batch".into()),
                refs: Vec::new(), payload: serde_json::json!({
                    "host_transcript_schema":1,"graph_id":"graph-460-pg","node_id":"node-460-pg","attempt":1,
                    "session_id":"session-460-pg","turn_id":"turn-460-pg","ticket_digest":digest,
                    "body":{"content_schema":2,"sha256":artifact.sha256,"bytes":artifact.bytes,
                        "body":{"kind":"artifact_ref","artifact":artifact}},
                }),
            }.with_activity_binding(binding).unwrap(),
            schema_version: 2, idempotency_key: Some(key.clone()),
        };
        let origin = runtime::PreparedArtifactOrigin::EventBody {
            stream_id: input.event.stream_id.clone(),
            idempotency_key: key,
            kind: input.event.kind.clone(),
        };
        (input, artifact, origin)
    }

    fn persist_untrusted_header(
        backend: &PostgresRuntimeEventStore,
        input: RuntimeTransactionEventInput,
    ) {
        backend
            .append_transaction(AppendTransactionRequest {
                transaction_id: input.idempotency_key.clone().unwrap(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: input.event.stream_id.clone(),
                    expected_revision: backend.stream_revision(&input.event.stream_id).unwrap(),
                }],
                events: vec![input],
            })
            .unwrap();
    }

    fn assert_probe(
        backend: &PostgresRuntimeEventStore,
        origin: &runtime::PreparedArtifactOrigin,
        artifact: &runtime::ArtifactRef,
        expected: runtime::PreparedContentRoot,
        queries: u64,
    ) {
        let before = backend.executor().health();
        let result = backend.prepared_content_root(origin, artifact);
        let after = backend.executor().health();
        assert_eq!(
            after.metrics.checkout_count - before.metrics.checkout_count,
            1
        );
        assert_eq!(
            after.metrics.checkout_timeout_count,
            before.metrics.checkout_timeout_count
        );
        assert_eq!(
            after.metrics.query_count - before.metrics.query_count,
            queries
        );
        assert!(after.lanes.iter().all(|lane| lane.active_connections == 0));
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_460_pg_host_absent_never_borrows_generic_artifact_root_single_connection() {
        let scope = PostgresTestScope::new();
        let backend = backend(&scope);
        let (_, artifact, origin) = header(&"1".repeat(64));
        backend
            .append(RuntimeEventInput {
                stream_id: "untrusted-generic-ref-460".into(),
                scope: RuntimeEventScope::Session,
                kind: "fixture.untrusted_artifact_ref".into(),
                actor: None,
                status: None,
                refs: vec![runtime::RuntimeEventRef {
                    kind: "artifact".into(),
                    id: artifact.selector.clone(),
                }],
                payload: serde_json::json!({}),
            })
            .unwrap();
        assert_probe(
            &backend,
            &origin,
            &artifact,
            runtime::PreparedContentRoot::Absent,
            1,
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_460_pg_host_missing_atomic_pair_and_wrong_slot_block_without_nested_checkout() {
        let scope = PostgresTestScope::new();
        let backend = backend(&scope);
        let (input, artifact, origin) = header(&"2".repeat(64));
        persist_untrusted_header(&backend, input);
        assert_probe(
            &backend,
            &origin,
            &artifact,
            runtime::PreparedContentRoot::Blocked,
            4,
        );
        let (mut input, artifact, origin) = header(&"3".repeat(64));
        let body = input
            .event
            .payload
            .as_object_mut()
            .unwrap()
            .remove("body")
            .unwrap();
        input.event.payload["wrong_slot"] = body;
        persist_untrusted_header(&backend, input);
        assert_probe(
            &backend,
            &origin,
            &artifact,
            runtime::PreparedContentRoot::Blocked,
            2,
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_460_pg_host_root_bounds_primary_before_hydration() {
        let scope = PostgresTestScope::new();
        let backend = backend(&scope);
        let (mut input, artifact, origin) = header(&"4".repeat(64));
        input.event.payload["untrusted_large_field"] = serde_json::json!("x".repeat(1024 * 1024));
        persist_untrusted_header(&backend, input);
        let before = backend.executor().health();
        let error = backend
            .prepared_content_root(&origin, &artifact)
            .unwrap_err();
        let after = backend.executor().health();
        assert!(
            matches!(error, RuntimeEventStoreError::HistoryBudgetExceeded(ref identity)
            if identity == "Host transcript root event")
        );
        assert_eq!(
            after.metrics.checkout_count - before.metrics.checkout_count,
            1
        );
        assert_eq!(
            after.metrics.checkout_timeout_count,
            before.metrics.checkout_timeout_count
        );
        assert_eq!(after.metrics.query_count - before.metrics.query_count, 1);
    }
}

mod pg_root_checkout_453_tests {
    use super::*;

    fn single_connection_store(fixture: &PostgresTestScope) -> PostgresRuntimeEventStore {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("isolated test PostgreSQL required");
        let resolver = StaticSecretRefResolver::new([("fixture.pg.453".into(), url)]);
        let mut config = PostgresConnectionConfig::new(
            "pg-root-single-453",
            "fixture.pg.453",
            "cowd-root-checkout-453",
        );
        config.max_connections = 1;
        config.min_idle_connections = Some(1);
        config.checkout_timeout_ms = 1_000;
        let executor = fixture.bind(&PostgresExecutor::connect(config, &resolver).unwrap());
        let backend = PostgresRuntimeEventStore::new(executor).unwrap();
        let health = backend.executor().health();
        assert_eq!(health.max_connections, 1);
        assert_eq!(
            health
                .lanes
                .iter()
                .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
                .unwrap()
                .max_connections,
            1
        );
        backend
    }

    fn origin(call: &str) -> runtime::PreparedArtifactOrigin {
        runtime::PreparedArtifactOrigin::ToolEffectClosure {
            stream_id: format!("execution-effect:{call}"),
            idempotency_key: format!("{call}:receipt"),
        }
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_723_pg_history_checkpoint_schema5_root_keeps_scope_source_and_absent_contract() {
        let fixture = PostgresTestScope::new();
        let backend = single_connection_store(&fixture);
        let (mut artifact, _) = storage_pair("history-root-723");
        artifact.media_type = "application/json".into();
        artifact.bytes = 20_000;
        let projection_id = "projection-history:root-723";
        let origin = runtime::PreparedArtifactOrigin::CheckpointBody {
            projection_id: projection_id.into(),
            source_cursor: 7,
        };
        let mut revision = 0;
        for schema in [3, 4, 5, 6] {
            let payload = serde_json::json!({
                "schema": schema, "scope": artifact.visibility_scope,
                "body": {"content_schema": 2, "sha256": artifact.sha256, "bytes": artifact.bytes,
                    "body": {"kind": "artifact_ref", "artifact": artifact}}
            });
            let saved = backend.compare_and_put_projection_checkpoint(
                projection_id, 7, revision, &payload, 0,
            ).unwrap();
            revision = saved.revision;
            assert_eq!(backend.prepared_content_root(&origin, &artifact).unwrap(),
                if schema == 6 { runtime::PreparedContentRoot::Blocked }
                else { runtime::PreparedContentRoot::Checkpoint });
            let foreign_source = runtime::PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection_id.into(),
                source_cursor: 8,
            };
            assert_eq!(backend.prepared_content_root(&foreign_source, &artifact).unwrap(),
                runtime::PreparedContentRoot::Blocked);
            let mut foreign_scope = artifact.clone();
            foreign_scope.visibility_scope = "session:foreign".into();
            assert_eq!(backend.prepared_content_root(&origin, &foreign_scope).unwrap(),
                runtime::PreparedContentRoot::Blocked);
        }
        backend.delete_projection_checkpoint(projection_id).unwrap();
        assert_eq!(backend.prepared_content_root(&origin, &artifact).unwrap(),
            runtime::PreparedContentRoot::Absent);
    }

    #[test]
    fn n2_pg_program_cache_versions_keep_exact_schema_scope_and_source() {
        use sha2::{Digest, Sha256};
        let fixture = PostgresTestScope::new();
        let backend = single_connection_store(&fixture);
        let (mut artifact, _) = storage_pair("program-cache-991");
        artifact.media_type = "application/json".into();
        for (version, expected_schema) in [(1, 2), (2, 3)] {
            let projection_id = format!(
                "runtime:agentic-read-model:v{version}:{:x}",
                Sha256::digest(b"program-cache-991")
            );
            let origin = runtime::PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection_id.clone(),
                source_cursor: 7,
            };
            let payload = serde_json::json!({
                "schema_version":expected_schema, "session_id":"session-453",
                "body":{"content_schema":2,"sha256":artifact.sha256,"bytes":artifact.bytes,
                    "body":{"kind":"artifact_ref","artifact":artifact}},
            });
            let saved = backend
                .compare_and_put_projection_checkpoint(&projection_id, 7, 0, &payload, 0)
                .unwrap();
            assert_eq!(
                backend.prepared_content_root(&origin, &artifact).unwrap(),
                runtime::PreparedContentRoot::Checkpoint
            );
            let foreign_source = runtime::PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection_id.clone(),
                source_cursor: 8,
            };
            assert_eq!(
                backend
                    .prepared_content_root(&foreign_source, &artifact)
                    .unwrap(),
                runtime::PreparedContentRoot::Blocked
            );
            let mut foreign_scope = artifact.clone();
            foreign_scope.visibility_scope = "session:foreign".into();
            assert_eq!(
                backend
                    .prepared_content_root(&origin, &foreign_scope)
                    .unwrap(),
                runtime::PreparedContentRoot::Blocked
            );
            let mut crossed = payload;
            crossed["schema_version"] = serde_json::json!(if expected_schema == 2 { 3 } else { 2 });
            backend
                .compare_and_put_projection_checkpoint(
                    &projection_id,
                    7,
                    saved.revision,
                    &crossed,
                    0,
                )
                .unwrap();
            assert_eq!(
                backend.prepared_content_root(&origin, &artifact).unwrap(),
                runtime::PreparedContentRoot::Blocked,
                "another known cache schema must not authenticate this owner version"
            );
            backend
                .delete_projection_checkpoint(&projection_id)
                .unwrap();
            assert_eq!(
                backend.prepared_content_root(&origin, &artifact).unwrap(),
                runtime::PreparedContentRoot::Absent
            );
        }
    }

    fn storage_pair(call: &str) -> (runtime::ArtifactRef, AppendTransactionRequest) {
        let artifact = runtime::ArtifactRef {
            selector: format!("artifact://art_{call}"),
            sha256: format!("sha256:{}", "a".repeat(64)),
            bytes: 32,
            media_type: "application/vnd.cowd.process-closure+json".into(),
            visibility_scope: "session:session-453".into(),
            durability: harness_contract::context::EvidenceDurability::Durable,
        };
        let primary = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: format!("execution-effect:{call}"),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.effect.receipt".into(),
                status: Some("completed".into()),
                actor: Some("governed_tool".into()),
                refs: vec![runtime::RuntimeEventRef {
                    kind: "agent_attempt".into(),
                    id: "1".into(),
                }],
                payload: serde_json::json!({
                    "idempotency_key": call,
                    "process_admission_ref": format!("{call}:process-admission"),
                    "process_closure": {
                        "content_schema": 2, "sha256": artifact.sha256, "bytes": artifact.bytes,
                        "body": {"kind": "artifact_ref", "artifact": artifact}
                    },
                    "replay": {
                        "session_id": "session-453",
                        "parent_execution": {"execution_id": format!("graph-{call}"), "node_id": "node-453"}
                    },
                    "root_effect_owner": null,
                    "root_effect_context": null,
                    "effect": {"spawns_process": true},
                    "resolved_effect_scope": {"version": 2},
                    "outcome": {"status": "executed"}
                }),
            },
            schema_version: 1,
            idempotency_key: Some(format!("{call}:receipt")),
        };
        let mut index = primary.clone();
        index.event.stream_id = format!("execution-agent-receipts:graph-{call}:node-453:1");
        index.event.kind = "execution.agent_tool.receipt".into();
        index
            .event
            .payload
            .as_object_mut()
            .unwrap()
            .remove("replay");
        index.idempotency_key = Some(format!("agent-tool-receipt:{call}"));
        let expected_streams = [&primary, &index]
            .into_iter()
            .map(|event| ExpectedStreamRevision {
                stream_id: event.event.stream_id.clone(),
                expected_revision: 0,
            })
            .collect();
        (
            artifact,
            AppendTransactionRequest {
                transaction_id: format!("tool-effect-receipt:{call}"),
                expected_streams,
                events: vec![primary, index],
            },
        )
    }

    fn assert_root(
        backend: &PostgresRuntimeEventStore,
        origin: &runtime::PreparedArtifactOrigin,
        artifact: &runtime::ArtifactRef,
        expected: runtime::PreparedContentRoot,
        queries: u64,
    ) {
        let before = backend.executor().health();
        let result = backend.prepared_content_root(origin, artifact);
        let after = backend.executor().health();
        assert_eq!(
            after.metrics.checkout_count - before.metrics.checkout_count,
            1
        );
        assert_eq!(
            after.metrics.checkout_timeout_count,
            before.metrics.checkout_timeout_count
        );
        assert_eq!(
            after.metrics.query_count - before.metrics.query_count,
            queries
        );
        assert!(after.lanes.iter().all(|lane| lane.active_connections == 0));
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_453_pg_root_absent_single_online_connection() {
        let fixture = PostgresTestScope::new();
        let backend = single_connection_store(&fixture);
        let (artifact, _) = storage_pair("absent-453");
        assert_root(
            &backend,
            &origin("absent-453"),
            &artifact,
            runtime::PreparedContentRoot::Absent,
            2,
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_453_pg_root_pair_storage_primitive_single_connection() {
        let fixture = PostgresTestScope::new();
        let backend = single_connection_store(&fixture);
        let (artifact, pair) = storage_pair("canonical-453");
        backend.append_transaction(pair).unwrap();
        assert_root(
            &backend,
            &origin("canonical-453"),
            &artifact,
            runtime::PreparedContentRoot::Canonical,
            4,
        );
        let mut unrelated = artifact.clone();
        unrelated.selector = "artifact://unrelated-453".into();
        assert_root(
            &backend,
            &origin("canonical-453"),
            &unrelated,
            runtime::PreparedContentRoot::Absent,
            5,
        );

        let (artifact, mut pair) = storage_pair("foreign-index-453");
        pair.events[1].event.payload["outcome"] = serde_json::json!({"status": "failed"});
        backend.append_transaction(pair).unwrap();
        assert_root(
            &backend,
            &origin("foreign-index-453"),
            &artifact,
            runtime::PreparedContentRoot::Blocked,
            4,
        );

        let (artifact, mut pair) = storage_pair("missing-index-453");
        pair.events.truncate(1);
        pair.expected_streams.truncate(1);
        backend.append_transaction(pair).unwrap();
        assert_root(
            &backend,
            &origin("missing-index-453"),
            &artifact,
            runtime::PreparedContentRoot::Blocked,
            3,
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_453_pg_root_budget_before_hydration_single_connection() {
        let fixture = PostgresTestScope::new();
        let backend = single_connection_store(&fixture);
        let (artifact, mut pair) = storage_pair("oversize-453");
        pair.events[0].event.payload["oversize"] = serde_json::json!("x".repeat(1024 * 1024));
        backend.append_transaction(pair).unwrap();
        let before = backend.executor().health();
        let error = backend
            .prepared_content_root(&origin("oversize-453"), &artifact)
            .unwrap_err();
        let after = backend.executor().health();
        assert!(
            matches!(error, RuntimeEventStoreError::HistoryBudgetExceeded(ref identity)
            if identity == "process closure root event")
        );
        assert_eq!(
            after.metrics.checkout_count - before.metrics.checkout_count,
            1
        );
        assert_eq!(
            after.metrics.checkout_timeout_count,
            before.metrics.checkout_timeout_count
        );
        assert_eq!(after.metrics.query_count - before.metrics.query_count, 1);
        assert!(after.lanes.iter().all(|lane| lane.active_connections == 0));
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_453_pg_root_concurrent_absence_single_connection() {
        let fixture = PostgresTestScope::new();
        let backend = Arc::new(single_connection_store(&fixture));
        let before = backend.executor().health();
        let gate = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|index| {
                let backend = backend.clone();
                let gate = gate.clone();
                thread::spawn(move || {
                    let call = format!("absent-concurrent-453-{index}");
                    let (artifact, _) = storage_pair(&call);
                    gate.wait();
                    backend.prepared_content_root(&origin(&call), &artifact)
                })
            })
            .collect::<Vec<_>>();
        gate.wait();
        let outcomes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        let after = backend.executor().health();
        assert_eq!(
            after.metrics.checkout_count - before.metrics.checkout_count,
            2
        );
        assert_eq!(
            after.metrics.checkout_timeout_count,
            before.metrics.checkout_timeout_count
        );
        assert_eq!(after.metrics.query_count - before.metrics.query_count, 4);
        assert!(after.lanes.iter().all(|lane| lane.active_connections == 0));
        for outcome in outcomes {
            assert_eq!(outcome.unwrap(), runtime::PreparedContentRoot::Absent);
        }
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn own858_transparent_prepared_wrapper_preserves_fence_and_both_cas_faults() {
    let fixture = PostgresTestScope::new();
    for fenced in [false, true] {
        for phase in ["before", "after"] {
            let backend = PreparedCommitResponseLoss {
                inner: PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap(),
                lose_response: std::sync::atomic::AtomicBool::new(false),
                injected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                checkpoint_fault: Some(phase),
                checkpoint_injected: std::sync::atomic::AtomicBool::new(false),
            };
            let source = backend.source_mutation_fence().unwrap();
            assert_eq!(source, backend.inner.source_mutation_fence().unwrap());
            let projection_id = format!("runtime:prepared-maintenance:858-{fenced}-{phase}");
            let payload = serde_json::json!({"progress":1});
            let result = if fenced {
                backend.compare_and_put_projection_checkpoint_fenced(&projection_id, 0, 0, &payload, 0, &source)
            } else {
                backend.compare_and_put_projection_checkpoint(&projection_id, 0, 0, &payload, 0)
            };
            assert!(matches!(result, Err(RuntimeEventStoreError::Corrupt(_))));
            assert!(backend.injected.load(std::sync::atomic::Ordering::SeqCst));
            let saved = backend.inner.projection_checkpoint(&projection_id).unwrap();
            assert_eq!(saved.is_some(), phase == "after");
            if let Some(saved) = saved {
                assert_eq!(saved.revision, 1);
                assert_eq!(saved.payload, payload);
            }
            assert_eq!(source, backend.source_mutation_fence().unwrap());
        }
    }
}

#[test]
fn n2_checked_source_commit_survives_response_loss_and_rejects_related_write() {
    struct Neutral;
    impl CheckedSourceValidator for Neutral {
        fn frame_digest(&self) -> &str {
            "pg-source-frame"
        }
        fn validate_tail(
            &self,
            source: &ExpectedStreamRevision,
            actual: u64,
            events: &[DurableRuntimeEvent],
        ) -> RuntimeEventStoreResult<()> {
            if events.iter().all(|event| event.kind == "neutral") {
                Ok(())
            } else {
                Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: source.stream_id.clone(),
                    expected: source.expected_revision,
                    actual,
                })
            }
        }
    }
    fn command(id: &str, head: u64, kind: &str) -> AppendTransactionRequest {
        AppendTransactionRequest {
            transaction_id: id.into(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "source".into(),
                expected_revision: head,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: "source".into(),
                    scope: RuntimeEventScope::Program,
                    kind: kind.into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                },
                idempotency_key: Some(id.into()),
                schema_version: 1,
            }],
        }
    }
    // Preserve this test's namespace on panic, including a migration failure.
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    fixture
        .reconnect()
        .apply_migrations(RUNTIME_EVENT_DOMAIN, &RUNTIME_EVENT_MIGRATIONS[..1])
        .unwrap();
    let store = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap();
    store
        .append_transaction(command("lease-1", 0, "neutral"))
        .unwrap();
    let original = CheckedAppendRequest {
        transaction: command("conclusion", 0, "conclusion"),
        frame_digest: "pg-source-frame".into(),
        budget: runtime::SourceCheckBudget::default(),
    };
    assert!(matches!(
        store.append_transaction(original.transaction.clone()),
        Err(RuntimeEventStoreError::StaleRevision { .. })
    ));
    let first = store
        .append_checked_transaction(original.clone(), &Neutral)
        .unwrap();
    assert_eq!(first.validation.events_read, 1);
    assert_eq!(first.validation.sources[0].expected_revision, 0);
    assert_eq!(first.validation.sources[0].committed_revision, 1);
    assert_eq!(first.commit.stream_revisions[0].expected_revision, 0);
    assert_eq!(first.commit.stream_revisions[0].committed_revision, 2);
    drop(store);
    let store = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap();
    store
        .append_transaction(command("lease-2", 2, "neutral"))
        .unwrap();
    let repeated = store
        .append_checked_transaction(original.clone(), &Neutral)
        .unwrap();
    assert!(repeated.commit.duplicate);
    assert_eq!(repeated.commit.commit_cursor, first.commit.commit_cursor);
    assert_eq!(
        repeated.commit.stream_revisions,
        first.commit.stream_revisions
    );
    assert_eq!(repeated.validation, first.validation);
    let mut changed = original.clone();
    changed.transaction.events[0].event.payload = serde_json::json!({"different": true});
    assert!(matches!(
        store.append_checked_transaction(changed, &Neutral),
        Err(RuntimeEventStoreError::TransactionConflict { .. })
    ));
    let mut related = original;
    related.transaction = command("second-conclusion", 1, "conclusion");
    assert!(matches!(
        store.append_checked_transaction(related.clone(), &Neutral),
        Err(RuntimeEventStoreError::StaleRevision { .. })
    ));
    related.budget.max_events = 1;
    assert!(matches!(
        store.append_checked_transaction(related, &Neutral),
        Err(RuntimeEventStoreError::HistoryBudgetExceeded(_))
    ));
    assert_eq!(store.stream_revision("source").unwrap(), 3);
    struct Pausing {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl CheckedSourceValidator for Pausing {
        fn frame_digest(&self) -> &str {
            "pg-source-frame"
        }
        fn validate_tail(
            &self,
            _: &ExpectedStreamRevision,
            _: u64,
            _: &[DurableRuntimeEvent],
        ) -> RuntimeEventStoreResult<()> {
            // A bounded test probe stops inside validation, with no store IO.
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_millis(500))
                .unwrap();
            Ok(())
        }
    }
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap();
    let worker = std::thread::spawn(move || {
        writer
            .append_checked_transaction(
                CheckedAppendRequest {
                    transaction: command("guarded", 3, "conclusion"),
                    frame_digest: "pg-source-frame".into(),
                    budget: runtime::SourceCheckBudget::default(),
                },
                &Pausing {
                    entered: entered_tx,
                    release: std::sync::Mutex::new(release_rx),
                },
            )
            .unwrap()
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let executor = fixture.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    let mut tx = connection.transaction().unwrap();
    let acquired: bool = tx
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
            &[&"cowd-runtime-event-stream:source"],
        )
        .unwrap()
        .get(0);
    assert!(
        !acquired,
        "source guard must remain held during pure validation"
    );
    tx.rollback().unwrap();
    release_tx.send(()).unwrap();
    let guarded = worker.join().unwrap();
    assert_eq!(guarded.commit.stream_revisions[0].committed_revision, 4);
    let later = store
        .append_transaction(command("later-related", 4, "related"))
        .unwrap();
    assert!(later.commit_cursor > guarded.commit.commit_cursor);
    assert_eq!(store.stream_revision("source").unwrap(), 5);
    drop(store);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn structured_task_pending_registration_is_atomic_indexed_and_survives_reconnect() {
    use harness_contract::{context::ArtifactRef, task::TaskApplicationProvenance};
    let fixture = PostgresTestScope::new();
    let backend = PostgresTaskStore::new(fixture.reconnect()).unwrap();
    let executor = backend.executor().clone();
    let tasks = backend.clone().into_task_service();
    let mut spec = policy_bound_task_spec("structured-pg", "Return a schema-validated result");
    let artifact = |name: &str, media: &str| {
        ArtifactRef::durable(
            name,
            format!("sha256:{}", "a".repeat(64)),
            32,
            media,
            "session:structured-pg",
        )
    };
    spec.application_provenance = Some(TaskApplicationProvenance {
        model: "test-model".into(),
        producer_id: "app:pg".into(),
        workspace_id: "workspace:pg".into(),
        surface: "panel".into(),
        request_id: "request:pg".into(),
        request_idempotency_key: Some("once:pg".into()),
        result_contract_id: "result:pg".into(),
        result_schema_id: "schema:pg".into(),
        result_schema_version: 1,
        result_schema_digest: "b".repeat(64),
        result_max_bytes: 1024,
        instruction_ref: artifact("artifact://pg-instruction", "text/plain"),
        input_ref: artifact("artifact://pg-input", "application/json"),
        result_schema_ref: artifact("artifact://pg-schema", "application/schema+json"),
        request_digest: "c".repeat(64),
    });
    let command = TaskCreateCommand {
        task_id: "task-structured-pg".into(),
        root_task_id: "task-structured-pg".into(),
        mission_id: "mission:pg".into(),
        kind: TaskKind::Root,
        origin: TaskOrigin::System,
        origin_session_id: "structured-pg".into(),
        origin_turn_id: "turn:pg".into(),
        parent_task_id: None,
        predecessor_task_id: None,
        mission_assignment: TaskMissionAssignment::ExplicitLocked,
        mission_assigned_by: "gateway.application_structured_task".into(),
        spec,
        evidence_refs: Vec::new(),
    };
    let created = tasks.create(command.clone()).unwrap();
    let pending = created.aggregate.structured_registration.as_ref().unwrap();
    assert!(pending.receipt.is_none());
    assert_eq!(pending.generation, 1);
    assert_eq!(pending.input_digest, format!("sha256:{}", "a".repeat(64)));
    assert_eq!(pending.schema_digest, "b".repeat(64));
    let mut connection = executor.checkout_online_read().unwrap();
    let row=connection.query_one("SELECT (SELECT count(*) FROM runtime_tasks WHERE task_id='task-structured-pg'), (SELECT count(*) FROM runtime_task_evidence_outbox WHERE task_id='task-structured-pg' AND projected_at_ms IS NULL), (SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname='idx_runtime_tasks_structured_recovery')",&[]).unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(row.get::<_, i64>(2), 1);
    drop(connection);
    tasks
        .mark_outbox_projected(&created.outbox.unwrap().outbox_id, 123)
        .unwrap();
    let reopened = PostgresTaskStore::new(fixture.reconnect()).unwrap();
    assert_eq!(
        reopened.structured_recovery_candidates("", 1).unwrap(),
        vec![created.aggregate.clone()]
    );
    let service = reopened.into_task_service();
    assert_eq!(
        service.create(command.clone()).unwrap().aggregate,
        created.aggregate
    );
    let reassignment = harness_contract::mission::TaskMissionAssignmentCommand {
        operation_id: "foreign-structured-mission-move".into(),
        workspace_id: "workspace:pg".into(),
        task_ids: vec![created.aggregate.task_id.clone()],
        target_mission_id: "foreign-mission".into(),
        assignment: TaskMissionAssignment::ExplicitLocked,
        actor: "operator".into(),
        expected_task_revisions: std::collections::BTreeMap::from([(
            created.aggregate.task_id.clone(),
            created.aggregate.revision,
        )]),
        evidence_refs: Vec::new(),
    };
    assert!(service.assign_mission_batch(&reassignment).is_err());
    assert_eq!(
        service.get("task-structured-pg").unwrap().unwrap(),
        created.aggregate
    );
    let mut changed = command;
    changed
        .spec
        .application_provenance
        .as_mut()
        .unwrap()
        .input_ref
        .sha256 = format!("sha256:{}", "d".repeat(64));
    assert!(service.create(changed).is_err());
    assert_eq!(
        service.get("task-structured-pg").unwrap().unwrap(),
        created.aggregate
    );
    assert!(backend
        .structured_recovery_candidates("task-structured-pg", 1)
        .unwrap()
        .is_empty());
}
