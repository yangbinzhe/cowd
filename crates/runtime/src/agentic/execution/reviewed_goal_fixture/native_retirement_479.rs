use super::*;
use crate::execution_core::graph::{
    ExecutionNodeBinding, NodeExecutionOutcome, NodeExecutionTicket,
};
use crate::runtime_event_store::*;
use harness_contract::agent_action::{AgentActionStatus, AgentAttemptMode, TaskAttemptFailInput};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

tokio::task_local! {
    static JOURNAL_479: Arc<RuntimeEventStore>;
}

pub(super) async fn scope_journal_763<Future: std::future::Future>(
    journal: Arc<RuntimeEventStore>, future: Future,
) -> Future::Output {
    JOURNAL_479.scope(journal, future).await
}

pub(super) fn selected_journal() -> Arc<RuntimeEventStore> {
    JOURNAL_479
        .try_with(Arc::clone)
        .unwrap_or_else(|_| Arc::new(RuntimeEventStore::for_test()))
}

#[derive(Debug, Clone, Copy)]
enum ClosureFault {
    Missing,
    WrongAttempt,
    ForeignReceipt,
    UnknownReceipt,
}

#[derive(Debug, Default)]
struct FaultControl {
    execution: String,
    hold_settlement: bool,
    fail_release_once: bool,
    closure: Option<(String, String, ClosureFault)>,
    race_source: Option<String>,
    attempted: Vec<AppendTransactionRequest>,
}

#[derive(Debug)]
struct RetirementJournal {
    inner: EphemeralRuntimeEventStore,
    control: Mutex<FaultControl>,
    store: std::sync::OnceLock<std::sync::Weak<RuntimeEventStore>>,
    closure_hits: AtomicUsize,
    race_hits: AtomicUsize,
    release_failure_hits: AtomicUsize,
}

impl RetirementJournal {
    fn new() -> (Arc<RuntimeEventStore>, Arc<Self>) {
        let backend = Arc::new(Self {
            inner: EphemeralRuntimeEventStore::new(),
            control: Mutex::new(FaultControl::default()),
            store: std::sync::OnceLock::new(),
            closure_hits: AtomicUsize::new(0),
            race_hits: AtomicUsize::new(0),
            release_failure_hits: AtomicUsize::new(0),
        });
        let store = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
        backend.store.set(Arc::downgrade(&store)).unwrap();
        (store, backend)
    }

    fn hold(&self, packet: &AgentTaskPacket) {
        let mut control = self.control.lock().unwrap();
        control.execution = packet.graph_id().to_string();
        control.hold_settlement = true;
    }

    fn allow(&self) {
        let mut control = self.control.lock().unwrap();
        control.hold_settlement = false;
        control.closure = None;
        control.race_source = None;
    }
}

macro_rules! delegate_retirement_ports {
    ($(fn $name:ident(&self $(, $argument:ident: $kind:ty)*) -> $result:ty;)*) => {
        $(fn $name(&self $(, $argument: $kind)*) -> $result {
            self.inner.$name($($argument),*)
        })*
    };
}

impl RuntimeEventStoreBackend for RetirementJournal {
    fn append_checked_transaction(
        &self,
        request: crate::CheckedAppendRequest,
        validator: &dyn crate::CheckedSourceValidator,
    ) -> RuntimeEventStoreResult<crate::CheckedAppendReceipt> {
        self.inner.append_checked_transaction(request, validator)
    }

    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        {
            let mut control = self.control.lock().unwrap();
            if control.fail_release_once
                && request.events.iter().any(|event| {
                    serde_json::from_value::<AgentActionEnvelope>(
                        event.event.payload["envelope"].clone(),
                    )
                    .is_ok_and(|envelope| {
                        envelope.actor.execution_id.as_deref() == Some(control.execution.as_str())
                            && matches!(envelope.action, AgentAction::TaskRelease(_))
                    })
                })
            {
                control.fail_release_once = false;
                self.release_failure_hits.fetch_add(1, Ordering::SeqCst);
                return Err(RuntimeEventStoreError::Corrupt(
                    "N1 injected original release transaction failure".into(),
                ));
            }
        }
        let (hold, race) = {
            let mut control = self.control.lock().unwrap();
            let selected = request.events.iter().any(|event| {
                serde_json::from_value::<AgentActionEnvelope>(event.event.payload["envelope"].clone())
                    .is_ok_and(|envelope| matches!(envelope.action,
                        AgentAction::TaskAttemptFail(input) if input.execution_id == control.execution))
            });
            if selected {
                control.attempted.push(request.clone());
                (control.hold_settlement, control.race_source.take())
            } else {
                (false, None)
            }
        };
        if hold {
            return Err(RuntimeEventStoreError::Corrupt(
                "479 final settlement append held".into(),
            ));
        }
        if let Some(stream_id) = race {
            let source = request
                .expected_streams
                .iter()
                .find(|source| source.stream_id == stream_id)
                .expect("the actual retirement transaction must fence the selected exit source")
                .clone();
            assert!(source.expected_revision > 0);
            self.inner.append_transaction(AppendTransactionRequest {
                transaction_id: format!("479-source-race:{}", request.transaction_id),
                expected_streams: vec![source],
                events: vec![RuntimeEventInput {
                    stream_id, scope: RuntimeEventScope::Agent,
                    kind: "test.479.concurrent_source_diagnostic".into(), status: None,
                    actor: Some("479-negative-only".into()), refs: Vec::new(),
                    payload: serde_json::json!({"fault": "advance a genuinely pinned source, never an exit"}),
                }.into()],
            })?;
            self.race_hits.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.append_transaction(request)
    }

    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        let original = self.inner.event_by_idempotency_key(stream_id, key)?;
        let fault = self
            .control
            .lock()
            .unwrap()
            .closure
            .as_ref()
            .filter(|(stream, selected, _)| stream == stream_id && selected == key)
            .map(|(_, _, fault)| *fault);
        let Some(fault) = fault else {
            return Ok(original);
        };
        let original =
            original.expect("negative closure fault must target a genuinely committed closure");
        self.closure_hits.fetch_add(1, Ordering::SeqCst);
        match fault {
            ClosureFault::Missing => Ok(None),
            ClosureFault::WrongAttempt
            | ClosureFault::ForeignReceipt
            | ClosureFault::UnknownReceipt => {
                let store = self.store.get().unwrap().upgrade().unwrap();
                let mut record = store
                    .resolve_read_record(original.clone())
                    .map_err(RuntimeEventStoreError::Corrupt)?;
                fn change_attempt(value: &mut serde_json::Value) -> usize {
                    match value {
                        serde_json::Value::Object(fields) => {
                            if let Some(closures) = fields
                                .get_mut("lease_closures")
                                .and_then(serde_json::Value::as_array_mut)
                            {
                                assert_eq!(closures.len(), 1);
                                let attempt = closures[0]["binding"]["attempt"].as_u64().unwrap();
                                closures[0]["binding"]["attempt"] =
                                    serde_json::json!(attempt.checked_add(1).unwrap());
                                1
                            } else {
                                fields.values_mut().map(change_attempt).sum()
                            }
                        }
                        _ => 0,
                    }
                }
                match fault {
                    ClosureFault::WrongAttempt => {
                        assert_eq!(change_attempt(&mut record.payload), 1)
                    }
                    ClosureFault::ForeignReceipt => {
                        record.payload["replay"]["parent_execution"]["execution_id"] =
                            serde_json::json!("foreign-execution")
                    }
                    ClosureFault::UnknownReceipt => {
                        record.payload["outcome"]["status"] = serde_json::json!("unknown")
                    }
                    ClosureFault::Missing => unreachable!(),
                }
                if record.scope != RuntimeEventScope::ExecutionGraph {
                    return Ok(Some(record));
                }
                record
                    .payload
                    .as_object_mut()
                    .unwrap()
                    .remove("_runtime_activity_binding");
                let bytes = serde_json::to_vec(&record.payload)?;
                assert!(
                    bytes.len() <= 16 * 1024,
                    "479 exact closure fault must fit the original inline envelope limit"
                );
                let mut corrupted = original;
                corrupted.payload["sha256"] =
                    serde_json::json!(crate::context_evidence::read_sha256(&bytes));
                corrupted.payload["bytes"] = serde_json::json!(bytes.len());
                corrupted.payload["body"] =
                    serde_json::json!({"kind": "inline", "value": record.payload});
                Ok(Some(corrupted))
            }
        }
    }

    delegate_retirement_ports! {
        fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64>;
        fn background_projection_capacity_hint(&self) -> usize;
        fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64>;
        fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn list_stream_after(&self, stream_id: &str, after_sequence: u64, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn position_page(&self, scope: RuntimeEventScope, identity: &str, kind: Option<&str>, after: Option<(u64, u32)>, through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn prepared_content_root(&self, origin: &crate::PreparedArtifactOrigin, artifact: &harness_contract::context::ArtifactRef) -> RuntimeEventStoreResult<crate::PreparedContentRoot>;
        fn transaction_stream_revision_exact(&self, transaction_id: &str, stream_id: &str) -> RuntimeEventStoreResult<Option<crate::CommittedStreamRevision>>;
        fn recent_event_lines(&self, stream_id: &str, references: &[RuntimeEventRef], through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String>;
        fn list_stream_before(&self, stream_id: &str, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn count_stream_kinds(&self, stream_id: &str, kinds: &[&str], through_sequence: u64) -> Result<u64, String>;
        fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String>;
        fn append_transaction_with_terminal(&self, request: AppendTransactionRequest, terminal: SessionTerminalInput) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
        fn consume_verified_decision_lease(&self, lease_id: &str, principal_id: &str, review_id: &str, action: &str, scope: &str, evidence_digest: &str, credential_epoch: u64, consumed_at_ms: u64) -> RuntimeEventStoreResult<()>;
        fn append_transaction_with_verified_decision_lease(&self, request: AppendTransactionRequest, lease: &crate::VerifiedDecisionLease) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
        fn append_batch_if_revision(&self, stream_id: String, expected_revision: u64, transaction_id: String, events: Vec<RuntimeTransactionEventInput>) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
        fn events_after_cursor(&self, cursor: u64, max_commits: usize) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>>;
        fn projection_scan_page(&self, cursor: u64, interest: &crate::RuntimeProjectionInterest, max_commits: usize, max_events: usize, max_bytes: usize) -> RuntimeEventStoreResult<crate::RuntimeProjectionScanPage>;
        fn projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>>;
        fn projection_checkpoints_with_prefix(&self, prefix: &str) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>>;
        fn put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
        fn compare_and_put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
        fn compare_and_repair_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
        fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool>;
        fn list_stream_page_desc(&self, stream_id: &str, limit: usize, offset: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn stream_event_count(&self, stream_id: &str) -> Result<usize, String>;
        fn execution_events_for_session(&self, session_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn events_for_root_execution(&self, root_execution_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn events_for_root_execution_kind(&self, root_execution_id: &str, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn events_for_activity(&self, activity_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn list_scope(&self, scope: RuntimeEventScope, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn list_scope_page_asc(&self, scope: RuntimeEventScope, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn list_scope_stream_prefix_page_asc(&self, scope: RuntimeEventScope, stream_prefix: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn list_scope_kind_page_asc(&self, scope: RuntimeEventScope, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        fn stream_ids_for_scope(&self, scope: RuntimeEventScope) -> RuntimeEventStoreResult<Vec<String>>;
        fn stream_ids_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<String>>;
        fn stream_ids_for_scope_kind_at_sequence_page(&self, scope: RuntimeEventScope, kind: &str, sequence: u64, after: Option<(u64, String)>, limit: usize) -> RuntimeEventStoreResult<Vec<(String, u64)>>;
        fn latest_stream_statuses_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>>;
        fn latest_for_stream(&self, stream_id: &str) -> Result<Option<DurableRuntimeEvent>, String>;
        fn latest_for_stream_kind(&self, stream_id: &str, kind: &str) -> Result<Option<DurableRuntimeEvent>, String>;
        fn enqueue_session_terminal(&self, terminal_id: &str, message_id: &str, session_id: &str, commit_cursor: u64, payload_ref: &str) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
        fn claim_session_terminals(&self, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
        fn session_terminal(&self, terminal_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
        fn session_terminal_for_execution(&self, session_id: &str, execution_id: &str, turn_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
        fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool>;
        fn materialized_session_terminals_after(&self, session_id: &str, after_commit_cursor: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
        fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth>;
        fn blocked_session_terminals(&self, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
        fn retry_session_terminal(&self, terminal_id: &str, actor: &str, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
        fn adopt_session_terminal_fence(&self, request: &RuntimeSessionTerminalFenceAdoption) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
        fn ack_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
        fn suppress_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
        fn fail_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, class: RuntimeSessionOutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
    }
}

struct NativeRetirementFixture {
    physical: PhysicalProcessFixture,
    wire: RootWireServer,
    backend: Arc<RetirementJournal>,
    worker: PhysicalWorker,
    paused_ticket: NodeExecutionTicket,
    goal_before: harness_contract::goal::GoalContract,
    resume: Arc<tokio::sync::Notify>,
    _cleanup: RealFixtureCleanup,
}

impl NativeRetirementFixture {
    async fn new(name: &str) -> Self {
        let (journal, backend) = RetirementJournal::new();
        let physical = Box::pin(JOURNAL_479.scope(
            journal,
            PhysicalProcessFixture::new(
                crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
                name,
            ),
        ))
        .await;
        let services = &physical.services;
        let cleanup = RealFixtureCleanup {
            services: services.clone(),
            host: physical.host.clone(),
        };
        let wire = install_physical_native_wire(services).await;
        let arrived = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *wire.pause.lock().unwrap() =
            Some((format!("{name}:held-read"), arrived.clone(), resume.clone()));
        let worker = dispatch_native_resource_worker(
            &physical,
            &wire,
            name,
            "479 original Native write",
            true,
        )
        .await;
        backend.hold(&worker.packet);
        tokio::time::timeout(Duration::from_secs(30), arrived.notified())
            .await
            .expect("actual Native claim must reach the original local-provider barrier");
        let run = services
            .agent_runtime()
            .get(worker.packet.agent_id())
            .unwrap();
        assert_eq!(run.run_id, worker.packet.run_id());
        assert_eq!(
            run.backend,
            crate::agent_run_handle::AgentBackendKind::InProcess
        );
        let program = services
            .agent_action_service()
            .project(&worker.root_actor.program_id)
            .unwrap();
        let task = &program.tasks[&worker.task_ref];
        assert_eq!(
            task.claim_execution_id.as_deref(),
            Some(worker.packet.graph_id())
        );
        assert_eq!(task.claim_generation, u64::from(worker.packet.attempt));
        assert_eq!(
            task.claimant.as_deref(),
            Some(
                worker
                    .packet
                    .agentic_binding
                    .as_ref()
                    .unwrap()
                    .agent_id
                    .as_str()
            )
        );
        assert!(task.active_attempts.contains_key(worker.packet.graph_id()));
        let attempt = &task.active_attempts[worker.packet.graph_id()];
        assert_eq!(attempt.generation + 1, u64::from(worker.packet.attempt));
        assert_eq!(
            attempt.agent_id,
            worker.packet.agentic_binding.as_ref().unwrap().agent_id
        );
        assert_eq!(
            attempt.membership_id,
            worker
                .packet
                .agentic_binding
                .as_ref()
                .unwrap()
                .membership_id
        );
        let compiled = worker.packet.binding.as_ref().unwrap();
        assert_eq!(
            crate::agent::binding::recompute_binding_digest(compiled).unwrap(),
            compiled.binding_digest
        );
        assert!(physical.host.calls.lock().unwrap().iter().any(|request| {
            request.tool_name == "task_claim"
                && request.step_binding.as_ref().is_some_and(|step| {
                    step.snapshot.identity.run_ref.as_deref() == Some(worker.packet.run_id())
                })
        }));
        assert!(
            crate::agent_runtime::confirmed_run_exit(services.event_store(), &worker.packet)
                .unwrap()
                .is_none()
        );
        let children = services
            .graph_state_store()
            .child_links(worker.packet.graph_id())
            .unwrap();
        let child = children
            .iter()
            .map(|link| {
                services
                    .graph_state_store()
                    .load(&link.child_execution_id)
                    .unwrap()
            })
            .find(|graph| {
                graph
                    .nodes
                    .iter()
                    .any(|node| node.kind == ExecutionNodeKind::InlineModel)
            })
            .expect("the original Native backend must register its actual Conversation child");
        let node = child
            .nodes
            .iter()
            .find(|node| {
                node.kind == ExecutionNodeKind::InlineModel
                    && child.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Running)
            })
            .unwrap();
        let binding = services
            .graph_state_store()
            .current_binding(
                &child,
                &node.id,
                child.recovery_cursor.node_attempts[&node.id],
            )
            .unwrap();
        let (paused_ticket, _) = services
            .graph_state_store()
            .binding_start_ticket(&child, &node.id, &binding)
            .unwrap();
        create_open_retirement_goal(services, &worker.root_actor);
        let goal_before = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        Self {
            physical,
            wire,
            backend,
            worker,
            paused_ticket,
            goal_before,
            resume,
            _cleanup: cleanup,
        }
    }

    async fn cancel_while_holding_settlement(&self) {
        let services = &self.physical.services;
        if services
            .agent_action_service()
            .project(&self.worker.root_actor.program_id)
            .unwrap()
            .tasks[&self.worker.task_ref]
            .status
            != crate::AgenticTaskStatus::CancelRequested
        {
            self.worker.withdraw().await;
        }
        let withdrawal = services
            .event_store()
            .event_by_idempotency_key(
                &format!("agentic-program:{}", self.worker.root_actor.program_id),
                &format!(
                    "agent-action:physical-withdraw:{}",
                    self.worker.packet.run_id()
                ),
            )
            .unwrap()
            .unwrap();
        let envelope: AgentActionEnvelope =
            serde_json::from_value(withdrawal.payload["envelope"].clone()).unwrap();
        assert!(matches!(envelope.action, AgentAction::TaskWithdraw(_)));
        let dispatched = services
            .dispatch_agentic_followups(
                &envelope,
                AgenticDispatchContext {
                    session_id: self.worker.packet.session_id().into(),
                    turn_id: "turn-dispatch".into(),
                    model_lease: "r1-native-wire".into(),
                    permission_ceiling: PermissionMode::WorkspaceWrite,
                    resource_scopes: r1_resource_write_scopes(),
                },
            )
            .await;
        assert!(
            dispatched.is_ok(),
            "withdrawal reconciliation must retain append failure for retry: {dispatched:?}"
        );
        tokio::time::timeout(
            Duration::from_secs(30),
            services.cancel_execution_tree(
                self.worker.packet.graph_id(),
                "479 original Native Task withdrawal",
            ),
        )
        .await
        .unwrap()
        .unwrap();
        self.worker.wait_exit().await;
        let mut last_retry = None;
        let mut first_rejection = None;
        let settlement = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                last_retry = Some(services
                    .retry_agentic_run_exit(self.worker.packet.graph_id())
                    .await);
                if first_rejection.is_none() {
                    if let Some(Err(error)) = &last_retry {
                        eprintln!("R1_479_FIRST_RETIREMENT_REJECTION graph={} task={} error={error}",
                            self.worker.packet.graph_id(), self.worker.task_ref);
                        first_rejection = Some(error.clone());
                    }
                }
                if !self.backend.control.lock().unwrap().attempted.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if let Err(error) = settlement {
            let graph = services.graph_state_store().load(self.worker.packet.graph_id());
            let physical = match &graph {
                Ok(graph) => match services.graph_state_store().current_binding(
                    graph, self.worker.packet.node_id(), self.worker.packet.attempt,
                ) {
                    Ok(binding) => services.commit_service().physical_closure_diagnostic(
                        graph, self.worker.packet.node_id(), &binding,
                    ),
                    Err(error) => serde_json::json!({"binding_error":error.to_string()}),
                },
                Err(error) => serde_json::json!({"graph_error":error.to_string()}),
            };
            let task = services.agent_action_service()
                .project(&self.worker.root_actor.program_id)
                .map(|program| serde_json::to_value(program.tasks.get(&self.worker.task_ref)))
                .map_err(|error| error.to_string());
            let task = match task {
                Ok(Ok(task)) => {
                    let bytes = serde_json::to_vec(&task).unwrap();
                    if bytes.len() <= 64 * 1024 {
                        task
                    } else {
                        serde_json::json!({
                            "omitted":"diagnostic_task_byte_budget",
                            "bytes":bytes.len(),
                            "sha256":crate::context_evidence::read_sha256(&bytes),
                        })
                    }
                }
                other => serde_json::json!({"task_error":format!("{other:?}")}),
            };
            let retry = serde_json::json!({"result":format!("{last_retry:?}")});
            let retry_bytes = serde_json::to_vec(&retry).unwrap();
            let retry = if retry_bytes.len() <= 16 * 1024 {
                retry
            } else {
                serde_json::json!({
                    "omitted":"diagnostic_retry_byte_budget",
                    "bytes":retry_bytes.len(),
                    "sha256":crate::context_evidence::read_sha256(&retry_bytes),
                })
            };
            eprintln!("OWN834_NATIVE_SETTLEMENT_UNPROVED {}", serde_json::json!({
                "task":task, "physical":physical, "last_original_retry":retry,
                "first_original_rejection":first_rejection,
            }));
            panic!(
                "genuine exit, native effect history and closure must reach original final Task append: {error}"
            );
        }
        assert_pending(services, &self.worker);
        tokio::time::timeout(
            Duration::from_secs(30),
            services.execution_supervisor().wait_for_quiescence(self.worker.packet.graph_id()),
        )
        .await
        .expect("original retirement execution must become quiescent")
        .expect("original retirement execution must finish its physical closure");
        assert_real_closures(services, self.worker.packet.graph_id());
    }

    async fn cold_rebuild(self) -> Self {
        let Self { physical, wire, backend, worker, paused_ticket, goal_before, resume, _cleanup } = self;
        let packet = worker.packet.clone();
        let task_ref = worker.task_ref.clone();
        let root_actor = worker.root_actor.clone();
        let PhysicalProcessFixture { services, spec, definition, _storage_root, .. } = physical;
        services.shutdown_maintenance().await;
        let shutdown = services.shutdown_execution().await;
        assert!(shutdown.errors.is_empty(), "{shutdown:?}");
        assert_eq!(shutdown.remaining_keys, 0, "{shutdown:?}");
        assert_eq!(shutdown.forced_aborts, 0, "{shutdown:?}");
        let original = Arc::downgrade(&services);
        let artifacts = services.artifact_store().clone();
        let tasks = services.task_aggregate_service().clone();
        let workspace = services.workspace_root().to_path_buf();
        let home = _storage_root.as_ref().unwrap().path().join("home");
        drop(worker);
        drop(_cleanup);
        drop(services);
        assert!(original.upgrade().is_none(), "cold recovery must lose the original Runtime owner");
        let host = Arc::new(ReviewReadHost::default());
        let services = RuntimeServices::builder(home, &workspace)
            .runtime_event_store(Arc::new(RuntimeEventStore::from_backend(backend.clone())))
            .artifact_store(artifacts)
            .task_aggregate_service(tasks)
            .tool_execution_host(host.clone())
            .build()
            .unwrap();
        host.services.set(Arc::downgrade(&services)).unwrap();
        let worker = PhysicalWorker {
            process: RealProcessSession { services: services.clone(), packet: packet.clone() },
            root_actor,
            packet,
            task_ref,
        };
        let cleanup = RealFixtureCleanup { services: services.clone(), host: host.clone() };
        Self {
            physical: PhysicalProcessFixture { services, spec, host, definition, _storage_root },
            wire, backend, worker, paused_ticket, goal_before, resume, _cleanup: cleanup,
        }
    }

    async fn finish(&self) {
        self.backend.allow();
        assert!(self
            .physical
            .services
            .retry_agentic_run_exit(self.worker.packet.graph_id())
            .await
            .unwrap());
        self.physical.services.shutdown_maintenance().await;
        self.physical.services.shutdown_execution().await;
    }
}

fn create_open_retirement_goal(services: &RuntimeServices, actor: &AgentActorBinding) {
    use harness_contract::goal::*;
    services
        .goal_store()
        .create(GoalContract {
            id: "goal:root-agentic-execution".into(),
            session_id: actor.session_id.clone(),
            objective: actor.objective_summary.clone(),
            criteria: vec![AcceptanceCriterion {
                id: "479-independent-delivery".into(),
                statement: "Cancellation is not accepted work".into(),
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: Vec::new(),
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".into(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: GoalScope::UserObjective,
            user_intent_criterion_id: Some("479-independent-delivery".into()),
            source_intent_ref: Some("session_message:dispatch".into()),
            execution_binding: Some(GoalExecutionBinding {
                objective_id: actor.objective_id.clone(),
                session_id: actor.session_id.clone(),
                turn_id: actor.turn_id.clone(),
                root_execution_id: actor.root_execution_id.clone().unwrap(),
                agentic_program_id: actor.program_id.clone(),
            }),
            spec_revision: 1,
            spec_digest: "479-cancellation-is-not-success".into(),
            review_refs: Vec::new(),
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        })
        .unwrap();
}

fn settlement_envelope(worker: &PhysicalWorker) -> AgentActionEnvelope {
    let mut actor = worker.root_actor.clone();
    actor.kind = AgentActorKind::Supervisor;
    actor.actor_id = "runtime.program-supervisor".into();
    actor.execution_id = Some(worker.packet.graph_id().into());
    actor.team_id = None;
    actor.agent_id = None;
    AgentActionEnvelope {
        action_id: format!(
            "runtime-attempt-settle:{}:{}:{}",
            actor.program_id,
            worker.task_ref,
            worker.packet.graph_id()
        ),
        actor,
        expected_revision: None,
        action: AgentAction::TaskAttemptFail(TaskAttemptFailInput {
            task_ref: worker.task_ref.clone(),
            execution_id: worker.packet.graph_id().into(),
            mode: AgentAttemptMode::Execute,
            reason: "original Agent local exit and retirement reconciliation".into(),
            retryable: false,
        }),
    }
}

fn assert_not_success(services: &RuntimeServices, worker: &PhysicalWorker) {
    let program = services
        .agent_action_service()
        .project(&worker.root_actor.program_id)
        .unwrap();
    assert_ne!(
        program.tasks[&worker.task_ref].status,
        crate::AgenticTaskStatus::Accepted
    );
    assert_ne!(program.status, crate::AgenticProgramStatus::Verified);
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    assert_eq!(
        goal.completion,
        harness_contract::goal::GoalCompletion::Open
    );
    assert!(goal.reviews.is_empty());
    assert!(program.objective_verdict.is_none());
}

fn assert_pending(services: &RuntimeServices, worker: &PhysicalWorker) {
    let program = services
        .agent_action_service()
        .project(&worker.root_actor.program_id)
        .unwrap();
    let task = &program.tasks[&worker.task_ref];
    assert_eq!(task.status, crate::AgenticTaskStatus::CancelRequested);
    assert_eq!(task.claim_generation, u64::from(worker.packet.attempt) + 1);
    assert_eq!(
        task.claim_execution_id.as_deref(),
        Some(worker.packet.graph_id())
    );
    assert!(task.claimant.is_some());
    assert!(task.active_attempts.contains_key(worker.packet.graph_id()));
    assert_not_success(services, worker);
}

fn assert_rejected_without_program_mutation(
    services: &RuntimeServices,
    worker: &PhysicalWorker,
    fragment: &str,
) {
    let before = services
        .agent_action_service()
        .project(&worker.root_actor.program_id)
        .unwrap();
    let goal_before = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let envelope = settlement_envelope(worker);
    let observation = services.agent_action_service().apply(&envelope).unwrap();
    assert_eq!(
        observation.status,
        AgentActionStatus::Rejected,
        "{observation:?}"
    );
    let error = observation.error.as_ref().unwrap();
    eprintln!("R1_479_RETIREMENT_REJECTION graph={} task={} error={error:?}",
        worker.packet.graph_id(), worker.task_ref);
    assert_eq!(error.code, "runtime_retirement_not_settled");
    assert!(
        serde_json::to_string(error).unwrap().contains(fragment),
        "{error:?}"
    );
    assert_eq!(
        services
            .agent_action_service()
            .project(&worker.root_actor.program_id)
            .unwrap(),
        before
    );
    assert_eq!(
        services.goal_store().get(&goal_before.id).unwrap().unwrap(),
        goal_before
    );
    assert!(services
        .event_store()
        .event_by_idempotency_key(
            &format!("agentic-program:{}", worker.root_actor.program_id),
            &format!("agent-action:{}", envelope.action_id),
        )
        .unwrap()
        .is_none());
    assert_pending(services, worker);
}

fn assert_real_closures(services: &RuntimeServices, execution: &str) -> usize {
    let mut pending = vec![execution.to_string()];
    let mut seen = BTreeSet::new();
    let mut starts_observed = 0;
    while let Some(graph_id) = pending.pop() {
        assert!(seen.insert(graph_id.clone()));
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert!(graph
            .node_statuses
            .values()
            .all(|status| status.is_terminal()));
        let mut after = 0;
        while after < graph.revision {
            let (next, starts) = services
                .graph_state_store()
                .binding_starts_page(&graph.id, after, graph.revision)
                .unwrap();
            assert!(next > after);
            for (node, binding, start) in starts {
                let (ticket, spec) = services
                    .graph_state_store()
                    .binding_start_ticket(&graph, &node, &binding)
                    .unwrap();
                assert_eq!(ticket.attempt, binding.attempt);
                assert_eq!(ticket.payload_ref, spec.payload_ref);
                assert!(
                    services
                        .graph_state_store()
                        .binding_lease_closure(&graph, &node, &binding, start)
                        .unwrap(),
                    "original owner must publish exact closure even for zero resource handles: {graph_id}/{node}"
                );
                starts_observed += 1;
            }
            after = next;
        }
        pending.extend(
            services
                .graph_state_store()
                .child_links(&graph_id)
                .unwrap()
                .into_iter()
                .map(|link| link.child_execution_id),
        );
    }
    assert!(
        starts_observed > 0,
        "closure checks must observe real admitted bindings"
    );
    starts_observed
}

async fn assert_withdrawn_once(fixture: &NativeRetirementFixture) {
    let services = &fixture.physical.services;
    fixture.backend.allow();
    assert!(services
        .retry_agentic_run_exit(fixture.worker.packet.graph_id())
        .await
        .unwrap());
    assert_withdrawn_evidence(fixture).await;
}

async fn assert_withdrawn_evidence(fixture: &NativeRetirementFixture) {
    let services = &fixture.physical.services;
    let program = services
        .agent_action_service()
        .project(&fixture.worker.root_actor.program_id)
        .unwrap();
    let task = &program.tasks[&fixture.worker.task_ref];
    assert_eq!(task.status, crate::AgenticTaskStatus::Withdrawn);
    assert_eq!(
        task.claim_generation,
        u64::from(fixture.worker.packet.attempt) + 1
    );
    assert!(task.claimant.is_none() && task.claim_execution_id.is_none());
    assert!(task.active_attempts.is_empty());
    assert_not_success(services, &fixture.worker);
    assert_eq!(
        services
            .goal_store()
            .get(&fixture.goal_before.id)
            .unwrap()
            .unwrap(),
        fixture.goal_before
    );
    let request = fixture
        .backend
        .control
        .lock()
        .unwrap()
        .attempted
        .last()
        .unwrap()
        .clone();
    let original: AgentActionEnvelope = request
        .events
        .iter()
        .find_map(|event| {
            serde_json::from_value::<AgentActionEnvelope>(event.event.payload["envelope"].clone())
                .ok()
                .filter(|envelope| matches!(envelope.action, AgentAction::TaskAttemptFail(_)))
        })
        .unwrap();
    let applied = services
        .event_store()
        .event_by_idempotency_key(
            &format!("agentic-program:{}", program.program_id),
            &format!("agent-action:{}", original.action_id),
        )
        .unwrap()
        .unwrap();
    assert_eq!(applied.transaction_id, request.transaction_id);
    assert!(request
        .expected_streams
        .iter()
        .any(|source| source.expected_revision > 0
            && source.stream_id == format!("agent:{}", fixture.worker.packet.agent_id())));
    assert!(request
        .expected_streams
        .iter()
        .any(|source| source.expected_revision > 0
            && source.stream_id == fixture.paused_ticket.graph_id));
    for _repeat in 0..2 {
        let replay = services.agent_action_service().apply(&original).unwrap();
        assert_eq!(replay.status, AgentActionStatus::Applied);
        assert!(replay.duplicate);
        assert_eq!(
            services
                .agent_action_service()
                .project(&program.program_id)
                .unwrap(),
            program
        );
    }
    let settlements = services
        .event_store()
        .list_stream(&format!("agentic-program:{}", program.program_id))
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.idempotency_key.as_deref()
                == Some(format!("agent-action:{}", original.action_id).as_str())
        })
        .count();
    assert_eq!(settlements, 1);
}

#[tokio::test]
async fn r1_479_native_started_cancel_withdraws_once_without_accepting_goal() {
    let fixture = NativeRetirementFixture::new("479-native-control").await;
    fixture.worker.withdraw().await;
    assert!(!fixture.physical.services
        .retry_agentic_run_exit(fixture.worker.packet.graph_id()).await.unwrap());
    assert_rejected_without_program_mutation(
        &fixture.physical.services, &fixture.worker, "exact local backend exit",
    );
    fixture.cancel_while_holding_settlement().await;
    let services = &fixture.physical.services;
    let stream = ExecutionNodeBinding::effect_stream_id(&fixture.paused_ticket);
    let original_head = services.event_store().stream_revision(&stream).unwrap();
    let original_events = services.event_store().list_stream(&stream).unwrap();
    let original_effect = services.commit_service()
        .inspect_execution_effect(&fixture.paused_ticket).unwrap();
    assert!(matches!(&original_effect,
        crate::execution_core::graph::ExecutionEffectState::Completed(_)
            | crate::execution_core::graph::ExecutionEffectState::Uncertain));
    let fixture = fixture.cold_rebuild().await;
    let services = &fixture.physical.services;
    assert_eq!(services.event_store().stream_revision(&stream).unwrap(), original_head);
    assert_eq!(services.event_store().list_stream(&stream).unwrap(), original_events);
    assert_eq!(services.commit_service()
        .inspect_execution_effect(&fixture.paused_ticket).unwrap(), original_effect);
    assert_pending(&fixture.physical.services, &fixture.worker);
    assert_withdrawn_once(&fixture).await;
    fixture.finish().await;
}

#[tokio::test]
async fn r1_479_native_cancelled_synthesis_exit_before_retirement_cold_replay() {
    let fixture = NativeRetirementFixture::new("479-native-exit-first").await;
    fixture.worker.withdraw().await;
    fixture.worker.cancel().await;
    fixture.resume.notify_one();
    fixture.worker.wait_exit().await;
    let services = &fixture.physical.services;
    let child = services.graph_state_store().load(&fixture.paused_ticket.graph_id).unwrap();
    let cancelled = child.nodes.iter().find_map(|node| {
        if node.kind != ExecutionNodeKind::Synthesize {
            return None;
        }
        child.node_results.get(&node.id).filter(|result| {
            result.status == ExecutionNodeStatus::Cancelled
                && result.result_ref.is_none()
                && result.evidence_refs.is_empty()
                && result.failure.as_ref().is_some_and(|failure| {
                    failure.kind == "terminal_presentation_cancelled"
                        && !failure.retryable && failure.evidence_refs.is_empty()
                })
        }).map(|result| (node.id.clone(), result.clone()))
    });
    assert!(cancelled.is_some(), "the real Native producer must reproduce candidate-3 cancelled Synthesize: {:?}", child.node_results);
    fixture.cancel_while_holding_settlement().await;
    let fixture = fixture.cold_rebuild().await;
    let recovered = fixture.physical.services.graph_state_store()
        .load(&fixture.paused_ticket.graph_id).unwrap();
    let (node_id, result) = cancelled.unwrap();
    assert_eq!(recovered.node_results.get(&node_id), Some(&result));
    assert_pending(&fixture.physical.services, &fixture.worker);
    assert_withdrawn_once(&fixture).await;
    fixture.finish().await;
}

#[tokio::test]
async fn r1_479_native_live_owner_cannot_settle() {
    let fixture = NativeRetirementFixture::new("479-live-owner").await;
    let services = &fixture.physical.services;
    let graph = services
        .graph_state_store()
        .load(&fixture.paused_ticket.graph_id)
        .unwrap();
    let binding = services
        .graph_state_store()
        .current_binding(
            &graph,
            &fixture.paused_ticket.node_id,
            fixture.paused_ticket.attempt,
        )
        .unwrap();
    let key = binding.lease_closure_key(&graph, &fixture.paused_ticket.node_id);
    let fence = key.rsplit(':').next().unwrap();
    assert_eq!(
        services
            .execution_supervisor()
            .active_binding_for_ticket_for_test(&fixture.paused_ticket)
            .await,
        Some(binding.clone())
    );
    let checker_hits = AtomicUsize::new(0);
    let owner = services.scope_locks().acquire_binding_owner(fence, || {
        checker_hits.fetch_add(1, Ordering::SeqCst);
        Err("479 must never acquire an admitted live owner's fence".into())
    });
    assert!(owner.is_err());
    assert_eq!(checker_hits.load(Ordering::SeqCst), 0);
    assert!(services
        .scope_locks()
        .try_recover_binding_owner(fence)
        .unwrap()
        .is_none());
    fixture.worker.withdraw().await;
    assert!(crate::agent_runtime::confirmed_run_exit(
        services.event_store(),
        &fixture.worker.packet
    )
    .unwrap()
    .is_none());
    assert_rejected_without_program_mutation(services, &fixture.worker, "exact local backend exit");
    fixture.cancel_while_holding_settlement().await;
    assert_withdrawn_once(&fixture).await;
    fixture.finish().await;
}

async fn reject_closure_fault(fault: ClosureFault, name: &str) {
    let fixture = NativeRetirementFixture::new(name).await;
    fixture.cancel_while_holding_settlement().await;
    let services = &fixture.physical.services;
    let graph = services
        .graph_state_store()
        .load(&fixture.paused_ticket.graph_id)
        .unwrap();
    let binding = services
        .graph_state_store()
        .current_binding(
            &graph,
            &fixture.paused_ticket.node_id,
            fixture.paused_ticket.attempt,
        )
        .unwrap();
    let key = binding.lease_closure_key(&graph, &fixture.paused_ticket.node_id);
    let before = fixture
        .backend
        .inner
        .event_by_idempotency_key(&graph.id, &key)
        .unwrap()
        .unwrap();
    fixture.backend.control.lock().unwrap().closure = Some((graph.id.clone(), key.clone(), fault));
    assert_rejected_without_program_mutation(services, &fixture.worker, "closure");
    assert!(fixture.backend.closure_hits.load(Ordering::SeqCst) > 0);
    assert_eq!(
        fixture
            .backend
            .inner
            .event_by_idempotency_key(&graph.id, &key)
            .unwrap()
            .unwrap(),
        before
    );
    fixture.backend.control.lock().unwrap().closure = None;
    assert_withdrawn_once(&fixture).await;
    fixture.finish().await;
}

#[tokio::test]
async fn r1_479_native_missing_closure_cannot_settle() {
    reject_closure_fault(ClosureFault::Missing, "479-missing-closure").await;
}

#[tokio::test]
async fn r1_479_native_wrong_attempt_closure_cannot_settle() {
    reject_closure_fault(ClosureFault::WrongAttempt, "479-wrong-attempt").await;
}

#[tokio::test]
async fn r1_479_native_source_cas_conflict_cannot_partially_withdraw() {
    let fixture = NativeRetirementFixture::new("479-source-cas").await;
    fixture.cancel_while_holding_settlement().await;
    let services = &fixture.physical.services;
    let stream = format!("agent:{}", fixture.worker.packet.agent_id());
    let source_before = services.event_store().stream_revision(&stream).unwrap();
    let program_before = services
        .agent_action_service()
        .project(&fixture.worker.root_actor.program_id)
        .unwrap();
    let goal_before = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    {
        let mut control = fixture.backend.control.lock().unwrap();
        control.hold_settlement = false;
        control.race_source = Some(stream.clone());
    }
    let error = services
        .agent_action_service()
        .apply(&settlement_envelope(&fixture.worker))
        .err()
        .expect("the actual backend source CAS must reject the entire settlement");
    assert!(
        matches!(error, crate::AgentActionServiceError::Store(RuntimeEventStoreError::StaleRevision {
        ref stream_id, expected, actual,
    }) if stream_id == &stream && expected == source_before && actual == source_before + 1),
        "{error}"
    );
    assert_eq!(fixture.backend.race_hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        services
            .agent_action_service()
            .project(&program_before.program_id)
            .unwrap(),
        program_before
    );
    assert_eq!(
        services.goal_store().get(&goal_before.id).unwrap().unwrap(),
        goal_before
    );
    assert_pending(services, &fixture.worker);
    assert!(services
        .event_store()
        .event_by_idempotency_key(
            &format!("agentic-program:{}", program_before.program_id),
            &format!(
                "agent-action:{}",
                settlement_envelope(&fixture.worker).action_id
            ),
        )
        .unwrap()
        .is_none());
    assert_withdrawn_once(&fixture).await;
    fixture.finish().await;
}

#[derive(Default)]
struct ScriptedWaitBackend {
    polls: AtomicUsize,
    publications: AtomicUsize,
    ticket: Mutex<Option<NodeExecutionTicket>>,
}

#[async_trait::async_trait]
impl crate::execution_core::graph::ScopedNodeBackend for ScriptedWaitBackend {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, crate::execution_core::graph::NodeExecutorError> {
        assert_eq!(self.polls.fetch_add(1, Ordering::SeqCst), 0);
        assert!(self
            .ticket
            .lock()
            .unwrap()
            .replace(ticket.clone())
            .is_none());
        Ok(NodeExecutionOutcome::new(
            harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::WaitingInput,
                result_ref: Some(format!(
                    "479-scripted-wait:{}:{}",
                    ticket.node_id, ticket.attempt
                )),
                wait_class: None,
                summary: Some("Private scripted backend awaits original Runner control".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: crate::tool_invocation_now_ms(),
            },
        ))
    }

    async fn after_commit(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<(), crate::execution_core::graph::NodeExecutorError> {
        assert_eq!(self.ticket.lock().unwrap().as_ref(), Some(ticket));
        assert_eq!(self.publications.fetch_add(1, Ordering::SeqCst), 0);
        Ok(())
    }
}

struct ScriptedWaitResolver {
    graph_id: String,
    previous_node_id: String,
    backend: Arc<ScriptedWaitBackend>,
}

impl crate::execution_core::graph::executors::ScopedNodeBackendResolver for ScriptedWaitResolver {
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<dyn crate::execution_core::graph::ScopedNodeBackend>> {
        (ticket.graph_id == self.graph_id && ticket.node_id != self.previous_node_id).then(|| {
            self.backend.clone() as Arc<dyn crate::execution_core::graph::ScopedNodeBackend>
        })
    }
}

#[tokio::test]
async fn r1_479_native_wait_result_then_cancel_preserves_original_receipt() {
    let fixture = NativeRetirementFixture::new("479-wait-then-cancel").await;
    let services = &fixture.physical.services;
    let backend = Arc::new(ScriptedWaitBackend::default());
    services
        .model_step_executor()
        .install_resolver(Arc::new(ScriptedWaitResolver {
            graph_id: fixture.paused_ticket.graph_id.clone(),
            previous_node_id: fixture.paused_ticket.node_id.clone(),
            backend: backend.clone(),
        }));
    fixture.resume.notify_one();
    let ticket = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let ticket = backend.ticket.lock().unwrap().clone();
            if let Some(ticket) = ticket {
                let graph = services.graph_state_store().load(&ticket.graph_id).unwrap();
                if graph.node_statuses.get(&ticket.node_id) == Some(&ExecutionNodeStatus::WaitingInput)
                    && backend.publications.load(Ordering::SeqCst) == 1 {
                    let binding = services.graph_state_store().current_binding(&graph, &ticket.node_id, ticket.attempt).unwrap();
                    let mut after = 0;
                    let mut closed = false;
                    while after < graph.revision {
                        let (next, starts) = services.graph_state_store().binding_starts_page(&graph.id, after, graph.revision).unwrap();
                        assert!(next > after);
                        for (node, original, start) in starts {
                            if node == ticket.node_id && original == binding {
                                closed = services.graph_state_store().binding_lease_closure(&graph, &node, &binding, start).unwrap();
                            }
                        }
                        after = next;
                    }
                    if closed { break ticket; }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("real Native claim/read must precede original Runner wait-result commit and callback closure");
    assert_eq!(backend.polls.load(Ordering::SeqCst), 1);
    assert!(fixture.wire.requests.lock().unwrap().len() >= 2);
    assert!(fixture
        .physical
        .host
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|request| request.tool_name == "read_file"));
    let graph = services.graph_state_store().load(&ticket.graph_id).unwrap();
    let binding = services
        .graph_state_store()
        .current_binding(&graph, &ticket.node_id, ticket.attempt)
        .unwrap();
    let original = services
        .graph_state_store()
        .committed_binding_result(&graph, &ticket.node_id, &binding)
        .unwrap()
        .unwrap();
    assert_eq!(original.0.status, ExecutionNodeStatus::WaitingInput);
    let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
    let effects_before = services.event_store().list_stream(&stream).unwrap();
    assert_eq!(effects_before.len(), 2);
    let receipt = services
        .event_store()
        .event_by_idempotency_key(&stream, &format!("{}:receipt", ticket.idempotency_key))
        .unwrap()
        .unwrap();
    assert_eq!(receipt.transaction_id, original.1.transaction_id);
    assert_eq!(receipt.commit_cursor, original.1.commit_cursor);
    fixture.cancel_while_holding_settlement().await;
    let cancelled = services.graph_state_store().load(&ticket.graph_id).unwrap();
    let retirement = services
        .graph_state_store()
        .binding_retirement_record(&cancelled, &ticket.node_id, &binding)
        .unwrap()
        .expect("original same-attempt WaitingInput -> Cancel command must be authenticated");
    assert!(retirement.sequence > original.1.sequence);
    assert!(retirement.commit_cursor > original.1.commit_cursor);
    assert_ne!(retirement.transaction_id, original.1.transaction_id);
    assert_eq!(
        cancelled.node_statuses[&ticket.node_id],
        ExecutionNodeStatus::Cancelled
    );
    assert_eq!(
        services
            .graph_state_store()
            .committed_binding_result(&cancelled, &ticket.node_id, &binding)
            .unwrap()
            .unwrap(),
        original
    );
    assert_eq!(
        services.event_store().list_stream(&stream).unwrap(),
        effects_before
    );
    assert!(
        matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(),
        crate::execution_core::graph::ExecutionEffectState::Completed(outcome)
            if outcome.result.status == ExecutionNodeStatus::WaitingInput)
    );
    assert_eq!(backend.publications.load(Ordering::SeqCst), 1);
    assert_withdrawn_once(&fixture).await;
    fixture.finish().await;
}

#[tokio::test]
async fn r1_479_native_unexited_physical_leaf_cannot_settle() {
    let fixture = NativeRetirementFixture::new("479-physical-leaf").await;
    let services = &fixture.physical.services;
    let hold = Arc::new(ReviewWriteHold::default());
    *fixture.physical.host.read_hold.lock().unwrap() = Some(hold.clone());
    fixture.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(30), hold.entered.notified())
        .await
        .expect("the original Native ToolHost leaf must physically enter the held read");
    let request = fixture
        .physical
        .host
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|request| request.tool_name == "read_file")
        .unwrap()
        .clone();
    assert_eq!(
        request.parent_execution_attempt,
        Some(fixture.worker.packet.attempt)
    );
    assert_eq!(
        request
            .step_binding
            .as_ref()
            .unwrap()
            .snapshot
            .identity
            .run_ref
            .as_deref(),
        Some(fixture.worker.packet.run_id())
    );
    fixture.worker.withdraw().await;
    let cancellation = {
        let services = services.clone();
        let graph_id = fixture.worker.packet.graph_id().to_string();
        tokio::spawn(async move {
            services
                .cancel_execution_tree(&graph_id, "479 leaf remains physically owned")
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!cancellation.is_finished());
    assert!(crate::agent_runtime::confirmed_run_exit(
        services.event_store(),
        &fixture.worker.packet
    )
    .unwrap()
    .is_none());
    assert!(fixture.backend.control.lock().unwrap().attempted.is_empty());
    assert_rejected_without_program_mutation(services, &fixture.worker, "exact local backend exit");
    let receipts = services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            fixture.worker.packet.graph_id(),
            fixture.worker.packet.node_id(),
            fixture.worker.packet.attempt,
        )
        .unwrap();
    assert!(!receipts
        .iter()
        .any(|receipt| receipt.outcome.tool_use_id == request.tool_use_id));
    hold.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), cancellation)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture.cancel_while_holding_settlement().await;
    assert_withdrawn_once(&fixture).await;
    fixture.finish().await;
}

struct ReleaseProcessGate(Arc<crate::agent_process_jsonl_adapter::ProcessExitGate>);

impl Drop for ReleaseProcessGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn r1_479_process_unreaped_owner_cannot_settle() {
    let (journal, backend) = RetirementJournal::new();
    let fixture = JOURNAL_479
        .scope(
            journal,
            PhysicalProcessFixture::new(
                crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
                "479-process-exit",
            ),
        )
        .await;
    let services = &fixture.services;
    let _cleanup = RealFixtureCleanup {
        services: services.clone(),
        host: fixture.host.clone(),
    };
    let worker = fixture
        .dispatch(
            "479-original-process",
            r1_resource_write_scopes(),
            true,
            false,
            true,
        )
        .await;
    backend.hold(&worker.packet);
    create_open_retirement_goal(services, &worker.root_actor);
    assert_eq!(
        services
            .agent_runtime()
            .get(worker.packet.agent_id())
            .unwrap()
            .backend,
        crate::agent_run_handle::AgentBackendKind::ProcessJsonl
    );
    let gate = services.agent_runtime().process_fixture_exit_gate(false);
    let _release_gate = ReleaseProcessGate(gate.clone());
    worker.finish_only().await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !gate.entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the original Process driver must hit its real exit-witness barrier");
    assert!(worker.directory().exists());
    assert!(
        crate::agent_runtime::confirmed_run_exit(services.event_store(), &worker.packet)
            .unwrap()
            .is_none()
    );
    assert_rejected_without_program_mutation(services, &worker, "exact local backend exit");
    gate.release();
    worker.wait_exit().await;
    backend.allow();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            services
                .retry_agentic_run_exit(worker.packet.graph_id())
                .await
                .unwrap();
            let program = services
                .agent_action_service()
                .project(&worker.root_actor.program_id)
                .unwrap();
            if program.tasks[&worker.task_ref].status == crate::AgenticTaskStatus::Withdrawn {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_not_success(services, &worker);
    assert_real_closures(services, worker.packet.graph_id());
    assert!(!worker.directory().exists());
    services.shutdown_maintenance().await;
    services.shutdown_execution().await;
}

#[path = "receipt_index_791_retirement_tests.rs"]
mod receipt_index_791_retirement_tests;

#[tokio::test]
async fn n1_native_retirement_recovers_automatically_after_fault_release() {
    automatic_retirement_after_fault(false).await;
}

#[tokio::test]
async fn n1_failed_control_receipt_retires_only_after_exact_physical_exit() {
    let fixture = Box::pin(NativeRetirementFixture::new("n1-failed-control")).await;
    let services = &fixture.physical.services;
    let worker = &fixture.worker;
    let backend = &fixture.backend;
    let arrived = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *fixture.wire.pause.lock().unwrap() = Some((
        "n1-after-failed-control".into(),
        arrived.clone(),
        resume.clone(),
    ));
    {
        let mut replies = fixture.wire.replies.lock().unwrap();
        replies.clear();
        for (id, tool, input) in [
            (
                "n1-failed-release",
                "task_release",
                serde_json::json!({"task_ref":worker.task_ref,"reason":"Release unfinished work"}),
            ),
            (
                "n1-after-failed-control",
                "read_file",
                serde_json::json!({"path":"out/a","complete":true}),
            ),
        ] {
            replies.push_back(serde_json::json!({"role":"assistant","content":null,
                "tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":tool,"arguments":input.to_string()}}]}));
        }
        replies.push_back(serde_json::json!({"role":"assistant","content":"The control failed; no work was accepted."}));
    }
    backend.control.lock().unwrap().fail_release_once = true;
    fixture.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(30), arrived.notified())
        .await
        .expect(
            "Native continuation must observe the failed control before the next held response",
        );
    assert_eq!(
        backend.release_failure_hits.load(Ordering::SeqCst),
        1,
        "original action transaction fault must be reached"
    );
    let receipts = services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            worker.packet.graph_id(),
            worker.packet.node_id(),
            worker.packet.attempt,
        )
        .unwrap();
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.outcome.tool_name == "task_release"
                && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Failed),
        "original commit owner must persist Failed receipt: {receipts:?}"
    );
    worker.withdraw().await;
    assert_rejected_without_program_mutation(services, worker, "exact local backend exit");
    worker.cancel().await;
    resume.notify_one();
    tokio::time::timeout(
        Duration::from_secs(30),
        services.cancel_execution_tree(
            worker.packet.graph_id(),
            "N1 failed local control retirement",
        ),
    )
    .await
    .unwrap()
    .unwrap();
    worker.wait_exit().await;
    assert_real_closures(services, worker.packet.graph_id());
    let index_source = format!(
        "execution-agent-receipts:{}:{}:{}",
        worker.packet.graph_id(),
        worker.packet.node_id(),
        worker.packet.attempt
    );
    let original = services
        .event_store()
        .list_stream(&index_source)
        .unwrap()
        .into_iter()
        .find(|event| {
            event.kind == "execution.agent_tool.receipt"
                && event.payload["outcome"]["tool_name"] == "task_release"
        })
        .expect("original Failed receipt index");
    let key = original.payload["idempotency_key"].as_str().unwrap();
    let primary_source = format!("execution-effect:{key}");
    for (source, selected, fault) in [
        (
            primary_source.clone(),
            format!("{key}:receipt"),
            ClosureFault::Missing,
        ),
        (
            primary_source.clone(),
            format!("{key}:receipt"),
            ClosureFault::ForeignReceipt,
        ),
        (
            primary_source.clone(),
            format!("{key}:receipt"),
            ClosureFault::UnknownReceipt,
        ),
        (
            index_source,
            format!("agent-tool-intent:{key}"),
            ClosureFault::Missing,
        ),
    ] {
        let hits = backend.closure_hits.load(Ordering::SeqCst);
        backend.control.lock().unwrap().closure = Some((source, selected, fault));
        assert_rejected_without_program_mutation(services, worker, "failed control receipt");
        assert!(
            backend.closure_hits.load(Ordering::SeqCst) > hits,
            "negative receipt fault must be read"
        );
        backend.control.lock().unwrap().closure = None;
    }
    backend.allow();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let program = services
                .agent_action_service()
                .project(&worker.root_actor.program_id)
                .unwrap();
            if program.tasks[&worker.task_ref].status == crate::AgenticTaskStatus::Withdrawn {
                assert!(program.tasks[&worker.task_ref].active_attempts.is_empty());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("original lane must retire physically closed failed control work");
    assert_eq!(
        services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap(),
        fixture.goal_before
    );
    assert_not_success(services, worker);
    services.shutdown_maintenance().await;
    let report = services.shutdown_execution().await;
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(report.remaining_keys, 0);
}

#[tokio::test]
async fn n1_native_retirement_recovers_automatically_after_cold_rebuild() {
    automatic_retirement_after_fault(true).await;
}

async fn automatic_retirement_after_fault(cold: bool) {
    let fixture = NativeRetirementFixture::new("n1-auto-retirement").await;
    fixture.worker.withdraw().await;
    fixture.worker.cancel().await;
    fixture.resume.notify_one();
    tokio::time::timeout(
        Duration::from_secs(30),
        fixture.physical.services.cancel_execution_tree(
            fixture.worker.packet.graph_id(),
            "N1 automatic retirement cancellation",
        ),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.worker.wait_exit().await;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if !fixture.backend.control.lock().unwrap().attempted.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("production lane must hit the held settlement fault");
    assert_pending(&fixture.physical.services, &fixture.worker);
    let fixture = if cold {
        fixture.cold_rebuild().await
    } else {
        fixture
    };
    fixture.backend.allow();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let program = fixture
                .physical
                .services
                .agent_action_service()
                .project(&fixture.worker.root_actor.program_id)
                .unwrap();
            if program.tasks[&fixture.worker.task_ref].status == crate::AgenticTaskStatus::Withdrawn
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("production lane must recover without test-driven retry");
    assert_withdrawn_evidence(&fixture).await;
    assert_real_closures(&fixture.physical.services, fixture.worker.packet.graph_id());
    fixture.physical.services.shutdown_maintenance().await;
    let report = fixture.physical.services.shutdown_execution().await;
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(report.forced_aborts, 0, "{report:?}");
    assert_eq!(report.remaining_keys, 0, "{report:?}");
}
