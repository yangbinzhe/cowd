//! Replayable projection from terminal Runtime output to governed knowledge.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness_contract::{
    agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus},
    core::TaskRisk,
    execution::ExecutionIdentity,
    execution_graph::{
        ExecutionGraph, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeStatus,
    },
    knowledge::{
        KnowledgeAuthority, KnowledgeCandidate, KnowledgeCandidateScope, KnowledgeCandidateState,
        KnowledgeLineage, KnowledgeNovelty,
    },
};
use sha2::{Digest, Sha256};

use crate::{
    AgentRunSnapshot, CancellationToken, L4PromotionService, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeTransactionEventInput,
};

const PROPOSAL_KIND: &str = "knowledge.candidate.proposed.v1";
const PROJECTOR_STREAM: &str = "knowledge-candidate-projector";
const PROJECTOR_BATCH: usize = 128;

/// Single replayable production consumer for terminal knowledge candidates.
pub struct KnowledgeCandidateProjector {
    event_store: Arc<RuntimeEventStore>,
    promotion: Arc<L4PromotionService>,
    cancellation: CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl KnowledgeCandidateProjector {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>, promotion: Arc<L4PromotionService>) -> Self {
        Self {
            event_store,
            promotion,
            cancellation: CancellationToken::new(),
            worker: Mutex::new(None),
        }
    }

    /// Start the commit-driven projector when Runtime is hosted by Tokio.
    ///
    /// Synchronous tests can call [`Self::run_once`] directly. Production
    /// composition starts exactly one worker per RuntimeServices instance.
    pub fn start(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        let projector = Arc::clone(self);
        *worker = Some(handle.spawn(async move {
            let mut commits = projector.event_store.subscribe_commits();
            loop {
                if let Err(error) = projector.run_once(PROJECTOR_BATCH).await {
                    tracing::warn!(%error, "knowledge candidate projector pass failed");
                }
                tokio::select! {
                    _ = projector.cancellation.cancelled() => break,
                    changed = commits.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        }));
    }

    pub async fn shutdown(&self) {
        self.cancellation.cancel();
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
    }

    /// Consume at most `max_commits` source commits and persist a durable
    /// cursor only after every relevant event in the window was dispositioned.
    pub async fn run_once(&self, max_commits: usize) -> Result<usize, String> {
        let cursor = self.cursor()?;
        let max_commits = max_commits.max(1);
        let mut scan_cursor = cursor;
        let mut last_cursor = cursor;
        let mut processed = 0;
        loop {
            let batches = self
                .event_store
                .events_after_cursor(scan_cursor, max_commits - processed)
                .map_err(|error| error.to_string())?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                scan_cursor = batch.commit_cursor;
                if is_projector_checkpoint_only(&batch) {
                    continue;
                }
                for event in &batch.events {
                    if event.kind == PROPOSAL_KIND {
                        match serde_json::from_value::<KnowledgeCandidate>(event.payload.clone()) {
                            Ok(candidate) => {
                                if let Err(error) = self.promotion.govern(candidate).await {
                                    self.dead_letter(
                                        event.event_id.as_str(),
                                        batch.commit_cursor,
                                        &error,
                                    )?;
                                }
                            }
                            Err(error) => {
                                self.dead_letter(
                                    event.event_id.as_str(),
                                    batch.commit_cursor,
                                    &error.to_string(),
                                )?;
                            }
                        }
                    }
                }
                last_cursor = batch.commit_cursor;
                processed += 1;
                if processed == max_commits {
                    break;
                }
            }
            if processed == max_commits {
                break;
            }
        }
        if last_cursor > cursor {
            self.checkpoint(last_cursor)?;
        }
        self.reconcile_pending().await?;
        Ok(processed)
    }

    fn cursor(&self) -> Result<u64, String> {
        Ok(self
            .event_store
            .latest_for_stream_kind(
                PROJECTOR_STREAM,
                "knowledge.candidate.projector.checkpoint.v1",
            )?
            .and_then(|event| {
                event
                    .payload
                    .get("source_cursor")
                    .and_then(serde_json::Value::as_u64)
            })
            .unwrap_or_default())
    }

    fn checkpoint(&self, source_cursor: u64) -> Result<(), String> {
        let key = format!("source-cursor:{source_cursor}");
        if self
            .event_store
            .event_by_idempotency_key(PROJECTOR_STREAM, &key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        let revision = self
            .event_store
            .stream_revision(PROJECTOR_STREAM)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                PROJECTOR_STREAM,
                revision,
                format!("knowledge-projector:{source_cursor}"),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: PROJECTOR_STREAM.to_string(),
                        scope: RuntimeEventScope::Recovery,
                        kind: "knowledge.candidate.projector.checkpoint.v1".to_string(),
                        status: Some("completed".to_string()),
                        actor: Some("runtime.knowledge_candidate_projector".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({"source_cursor": source_cursor}),
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn dead_letter(&self, event_id: &str, source_cursor: u64, error: &str) -> Result<(), String> {
        let key = format!("dead-letter:{event_id}");
        let revision = self
            .event_store
            .stream_revision(PROJECTOR_STREAM)
            .map_err(|store_error| store_error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                PROJECTOR_STREAM,
                revision,
                format!("knowledge-projector-dead-letter:{event_id}"),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: PROJECTOR_STREAM.to_string(),
                        scope: RuntimeEventScope::Recovery,
                        kind: "knowledge.candidate.projector.failed.v1".to_string(),
                        status: Some("blocked".to_string()),
                        actor: Some("runtime.knowledge_candidate_projector".to_string()),
                        refs: vec![RuntimeEventRef {
                            kind: "source_event".to_string(),
                            id: event_id.to_string(),
                        }],
                        payload: serde_json::json!({
                            "source_cursor": source_cursor,
                            "source_event_id": event_id,
                            "error": error,
                        }),
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            )
            .map_err(|store_error| store_error.to_string())?;
        Ok(())
    }

    async fn reconcile_pending(&self) -> Result<(), String> {
        for projection in self.promotion.list()? {
            if matches!(
                projection.state,
                KnowledgeCandidateState::AwaitingApproval | KnowledgeCandidateState::Blocked
            ) {
                self.promotion.govern(projection.candidate).await?;
            }
        }
        Ok(())
    }
}

/// Create the immutable proposal event committed alongside an Agent or Team
/// terminal transition.
pub(crate) fn candidate_proposal_event(
    candidate: KnowledgeCandidate,
) -> Result<RuntimeTransactionEventInput, String> {
    candidate.validate()?;
    let stream_id = format!("knowledge-candidate-inbox:{}", candidate.candidate_id);
    let idempotency_key = format!("proposal:{}", candidate.candidate_id);
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id,
            scope: RuntimeEventScope::Knowledge,
            kind: PROPOSAL_KIND.to_string(),
            status: Some("proposed".to_string()),
            actor: Some(candidate.producer.clone()),
            refs: identity_refs(&candidate),
            payload: serde_json::to_value(&candidate).map_err(|error| error.to_string())?,
        },
        idempotency_key: Some(idempotency_key),
        schema_version: 1,
    })
}

pub(crate) fn agent_terminal_candidate(
    snapshot: &AgentRunSnapshot,
    returned: &AgentReturnPacket,
) -> Option<KnowledgeCandidate> {
    if returned.status != AgentTerminalStatus::Completed
        || returned.outcome.trim().is_empty()
        || returned.failure.is_some()
    {
        return None;
    }
    let evidence_refs = returned
        .evidence_refs
        .iter()
        .filter(|evidence| evidence.is_durable())
        .map(|evidence| evidence.evidence_ref.clone())
        .collect::<Vec<_>>();
    if evidence_refs.is_empty() {
        return None;
    }
    Some(KnowledgeCandidate {
        candidate_id: stable_candidate_id(
            "agent",
            snapshot.run_id.as_str(),
            returned.outcome.as_str(),
        ),
        execution_identity: snapshot.execution_identity.clone(),
        scope: KnowledgeCandidateScope::AgentPrivate(snapshot.run_id.clone()),
        title: format!("Agent result for task {}", snapshot.task_id),
        claim: returned.outcome.trim().to_string(),
        evidence_refs: evidence_refs.clone(),
        authority: KnowledgeAuthority::AgentObservation,
        lineage: KnowledgeLineage {
            parent_candidate_ids: Vec::new(),
            source_refs: evidence_refs,
        },
        novelty: KnowledgeNovelty::New,
        risk: TaskRisk::Low,
        tags: vec![
            "agent-terminal".to_string(),
            format!("task:{}", snapshot.task_id),
        ],
        producer: "runtime.agent_terminal".to_string(),
        producer_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at_ms: snapshot.updated_at_ms,
    })
}

pub(crate) fn team_terminal_candidate(
    graph: &ExecutionGraph,
    node_id: &str,
    status: ExecutionNodeStatus,
    result: Option<&ExecutionNodeResult>,
) -> Option<KnowledgeCandidate> {
    if status != ExecutionNodeStatus::Completed {
        return None;
    }
    let node = graph.nodes.iter().find(|node| node.id == node_id)?;
    if node.kind != ExecutionNodeKind::Synthesize {
        return None;
    }
    let result = result?;
    let claim = result.summary.as_deref()?.trim();
    if claim.is_empty() {
        return None;
    }
    let evidence_refs = result
        .evidence_refs
        .iter()
        .filter(|evidence| evidence.is_durable())
        .map(|evidence| evidence.evidence_ref.clone())
        .collect::<Vec<_>>();
    if evidence_refs.is_empty() {
        return None;
    }
    let member_packet = graph.nodes.iter().find_map(|member| {
        (member.kind == ExecutionNodeKind::AgentTask)
            .then(|| serde_json::from_str::<AgentTaskPacket>(&member.payload_ref).ok())
            .flatten()
    })?;
    let team_id = member_packet.team_id()?.to_string();
    let task_graph = member_packet
        .assignment
        .execution_identity
        .task_graph_lineage()
        .ok()?;
    let execution_identity =
        ExecutionIdentity::for_team_node(&task_graph, team_id.clone(), node_id).ok()?;
    Some(KnowledgeCandidate {
        candidate_id: stable_candidate_id("team", team_id.as_str(), claim),
        execution_identity,
        scope: KnowledgeCandidateScope::Team(team_id.clone()),
        title: format!("Team synthesis for {}", member_packet.task_id()),
        claim: claim.to_string(),
        evidence_refs: evidence_refs.clone(),
        authority: KnowledgeAuthority::TeamSynthesis,
        lineage: KnowledgeLineage {
            parent_candidate_ids: Vec::new(),
            source_refs: evidence_refs,
        },
        novelty: KnowledgeNovelty::New,
        risk: TaskRisk::Medium,
        tags: vec![
            "team-terminal".to_string(),
            format!("team:{team_id}"),
            format!("task:{}", member_packet.task_id()),
        ],
        producer: "runtime.team_terminal".to_string(),
        producer_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at_ms: now_ms(),
    })
}

fn identity_refs(candidate: &KnowledgeCandidate) -> Vec<RuntimeEventRef> {
    let identity = &candidate.execution_identity;
    let mut refs = vec![
        RuntimeEventRef {
            kind: "workspace".to_string(),
            id: identity.workspace_id().to_string(),
        },
        RuntimeEventRef {
            kind: "knowledge_scope".to_string(),
            id: candidate.scope.key(),
        },
    ];
    for (kind, id) in [
        ("mission", identity.mission_id()),
        ("task", identity.task_id()),
        ("session", identity.session_id()),
        ("turn", identity.turn_id()),
        ("execution_graph", identity.graph_id()),
        ("team_run", identity.team_run_id()),
        ("agent_run", identity.agent_run_id()),
        ("execution_node", identity.node_id()),
    ] {
        if let Some(id) = id {
            refs.push(RuntimeEventRef {
                kind: kind.to_string(),
                id: id.to_string(),
            });
        }
    }
    refs
}

fn stable_candidate_id(kind: &str, owner: &str, claim: &str) -> String {
    let digest = format!(
        "{:x}",
        Sha256::digest(format!("{kind}:{owner}:{claim}").as_bytes())
    );
    format!("knowledge-{kind}-{}", &digest[..24])
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_projector_checkpoint_only(batch: &crate::CommittedEventBatch) -> bool {
    !batch.events.is_empty()
        && batch
            .events
            .iter()
            .all(|event| event.kind.ends_with(".projector.checkpoint.v1"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::reality::EvidenceRef;
    use memory::config::{BudgetConfig, StoreConfig};
    use memory::{CognitiveContextManager, MemoryConfig};

    async fn fixture() -> (
        tempfile::TempDir,
        Arc<RuntimeEventStore>,
        Arc<crate::ApprovalQueue>,
        Arc<L4PromotionService>,
    ) {
        let root = tempfile::tempdir().expect("runtime fixture");
        let mut memory_config = MemoryConfig {
            store: StoreConfig {
                sqlite_path: root.path().join("memory.sqlite"),
                blob_dir: root.path().join("memory-blobs"),
                enable_vector_index: false,
                ..Default::default()
            },
            budget: BudgetConfig {
                context_window: 16_000,
                reserved_system: 2_000,
                reserved_response: 1_000,
                ..Default::default()
            },
            ..Default::default()
        };
        memory_config.layers.l4_enabled = true;
        let memory = Arc::new(
            CognitiveContextManager::new(memory_config)
                .await
                .expect("memory manager"),
        );
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let approvals = Arc::new(crate::ApprovalQueue::new(Arc::clone(&events)));
        let promotion = Arc::new(L4PromotionService::new(
            Arc::clone(&events),
            Arc::clone(&approvals),
            Some(memory),
        ));
        (root, events, approvals, promotion)
    }

    fn task_graph() -> ExecutionIdentity {
        ExecutionIdentity::for_task_graph(
            "runtime-test",
            "workspace-test",
            "mission-test",
            "task-test",
            "session-test",
            "turn-test",
            "graph-test",
        )
        .expect("task graph identity")
    }

    fn private_candidate(candidate_id: &str, title: &str, claim: &str) -> KnowledgeCandidate {
        let run_id = format!("run-{candidate_id}");
        KnowledgeCandidate {
            candidate_id: candidate_id.to_string(),
            execution_identity: ExecutionIdentity::for_agent_node(
                &task_graph(),
                run_id.clone(),
                "agent-node",
            )
            .expect("agent identity"),
            scope: KnowledgeCandidateScope::AgentPrivate(run_id),
            title: title.to_string(),
            claim: claim.to_string(),
            evidence_refs: vec![EvidenceRef::observed(
                "tool_receipt",
                format!("evidence-{candidate_id}"),
            )],
            authority: KnowledgeAuthority::AgentObservation,
            lineage: KnowledgeLineage::default(),
            novelty: KnowledgeNovelty::New,
            risk: TaskRisk::Low,
            tags: vec!["projector-test".to_string()],
            producer: "runtime.projector.test".to_string(),
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at_ms: 1,
        }
    }

    fn team_candidate() -> KnowledgeCandidate {
        let team_id = "team-projector".to_string();
        KnowledgeCandidate {
            candidate_id: "candidate-team-projector".to_string(),
            execution_identity: ExecutionIdentity::for_team_node(
                &task_graph(),
                team_id.clone(),
                "synthesize",
            )
            .expect("team identity"),
            scope: KnowledgeCandidateScope::Team(team_id),
            title: "Team synthesis".to_string(),
            claim: "The reviewed team synthesis is reusable.".to_string(),
            evidence_refs: vec![EvidenceRef::observed("team_evidence", "team-evidence-1")],
            authority: KnowledgeAuthority::TeamSynthesis,
            lineage: KnowledgeLineage::default(),
            novelty: KnowledgeNovelty::New,
            risk: TaskRisk::Medium,
            tags: vec!["team-terminal".to_string()],
            producer: "runtime.projector.test".to_string(),
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at_ms: 1,
        }
    }

    fn append_candidate(events: &RuntimeEventStore, candidate: KnowledgeCandidate) {
        let proposal = candidate_proposal_event(candidate).expect("candidate event");
        events
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: format!(
                    "proposal-{}",
                    proposal
                        .event
                        .payload
                        .get("candidate_id")
                        .and_then(serde_json::Value::as_str)
                        .expect("candidate id")
                ),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: proposal.event.stream_id.clone(),
                    expected_revision: 0,
                }],
                events: vec![proposal],
            })
            .expect("candidate proposal commit");
    }

    #[tokio::test]
    async fn projector_advances_over_unrelated_commits_and_is_restart_idempotent() {
        let (_root, events, _approvals, promotion) = fixture().await;
        events
            .append(RuntimeEventInput {
                stream_id: "unrelated".to_string(),
                scope: RuntimeEventScope::Session,
                kind: "session.observed".to_string(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("unrelated event");
        let candidate = private_candidate(
            "candidate-private-projector",
            "Private observation",
            "A durable private observation.",
        );
        append_candidate(&events, candidate.clone());

        let projector =
            KnowledgeCandidateProjector::new(Arc::clone(&events), Arc::clone(&promotion));
        assert_eq!(projector.run_once(1).await.expect("first pass"), 1);
        assert!(promotion
            .get(&candidate.candidate_id)
            .expect("projection")
            .is_none());
        projector.run_once(8).await.expect("candidate pass");
        assert_eq!(
            promotion
                .get(&candidate.candidate_id)
                .expect("projection")
                .expect("candidate")
                .state,
            KnowledgeCandidateState::Promoted
        );

        let restarted =
            KnowledgeCandidateProjector::new(Arc::clone(&events), Arc::clone(&promotion));
        restarted.run_once(64).await.expect("restart pass");
        assert_eq!(
            promotion
                .list()
                .expect("candidate list")
                .into_iter()
                .filter(|item| item.candidate.candidate_id == candidate.candidate_id)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn projector_does_not_starve_source_commits_behind_its_checkpoint() {
        let (_root, events, _approvals, promotion) = fixture().await;
        events
            .append(RuntimeEventInput {
                stream_id: "unrelated".to_string(),
                scope: RuntimeEventScope::Session,
                kind: "session.observed".to_string(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("unrelated event");
        let projector =
            KnowledgeCandidateProjector::new(Arc::clone(&events), Arc::clone(&promotion));
        assert_eq!(projector.run_once(1).await.expect("checkpoint pass"), 1);

        let candidate = private_candidate(
            "candidate-after-checkpoint",
            "Observation after checkpoint",
            "A source commit behind the projector checkpoint must remain visible.",
        );
        append_candidate(&events, candidate.clone());

        assert_eq!(projector.run_once(1).await.expect("candidate pass"), 1);
        assert_eq!(
            promotion
                .get(&candidate.candidate_id)
                .expect("projection")
                .expect("candidate")
                .state,
            KnowledgeCandidateState::Promoted
        );
    }

    #[tokio::test]
    async fn projector_does_not_echo_another_projector_checkpoint() {
        let (_root, events, _approvals, promotion) = fixture().await;
        events
            .append(RuntimeEventInput {
                stream_id: "evolution-signal-projector".to_string(),
                scope: RuntimeEventScope::Evolution,
                kind: "evolution.signal.projector.checkpoint.v1".to_string(),
                status: Some("completed".to_string()),
                actor: Some("runtime.evolution_signal_projector".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"source_cursor": 1}),
            })
            .expect("foreign projector checkpoint");
        let projector =
            KnowledgeCandidateProjector::new(Arc::clone(&events), Arc::clone(&promotion));

        assert_eq!(projector.run_once(64).await.expect("projector pass"), 0);
        assert!(events
            .list_stream(PROJECTOR_STREAM)
            .expect("projector stream")
            .is_empty());
    }

    #[tokio::test]
    async fn team_candidate_waits_for_existing_approval_queue_then_promotes() {
        let (_root, events, approvals, promotion) = fixture().await;
        let candidate = team_candidate();
        append_candidate(&events, candidate.clone());
        let projector =
            KnowledgeCandidateProjector::new(Arc::clone(&events), Arc::clone(&promotion));
        projector.run_once(32).await.expect("proposal pass");
        let pending = promotion
            .get(&candidate.candidate_id)
            .expect("projection")
            .expect("pending candidate");
        assert_eq!(pending.state, KnowledgeCandidateState::AwaitingApproval);

        approvals
            .decide(
                &crate::security::test_human_interactive_principal(),
                crate::ApprovalDecisionCommand {
                    approval_id: pending.approval_id.expect("approval id"),
                    approved: true,
                    reason: "verified team evidence".to_string(),
                },
            )
            .expect("approval decision");
        let restarted = KnowledgeCandidateProjector::new(events, Arc::clone(&promotion));
        restarted.run_once(64).await.expect("approval pass");
        assert_eq!(
            promotion
                .get(&candidate.candidate_id)
                .expect("projection")
                .expect("promoted candidate")
                .state,
            KnowledgeCandidateState::Promoted
        );
    }

    #[tokio::test]
    async fn duplicate_is_superseded_and_conflict_is_approval_gated() {
        let (_root, events, _approvals, promotion) = fixture().await;
        let first = private_candidate("candidate-first", "Stable rule", "Use stable rule A.");
        let shared_identity = first.execution_identity.clone();
        let shared_scope = first.scope.clone();
        append_candidate(&events, first);
        let projector =
            KnowledgeCandidateProjector::new(Arc::clone(&events), Arc::clone(&promotion));
        projector.run_once(64).await.expect("first promotion");

        let mut duplicate =
            private_candidate("candidate-duplicate", "Another title", "Use stable rule A.");
        duplicate.execution_identity = shared_identity.clone();
        duplicate.scope = shared_scope.clone();
        append_candidate(&events, duplicate.clone());
        projector.run_once(64).await.expect("duplicate pass");
        assert_eq!(
            promotion
                .get(&duplicate.candidate_id)
                .expect("projection")
                .expect("duplicate")
                .state,
            KnowledgeCandidateState::Superseded
        );

        let mut conflict = private_candidate(
            "candidate-conflict",
            "Stable rule",
            "Use incompatible rule B.",
        );
        conflict.execution_identity = shared_identity;
        conflict.scope = shared_scope;
        append_candidate(&events, conflict.clone());
        projector.run_once(64).await.expect("conflict pass");
        let projection = promotion
            .get(&conflict.candidate_id)
            .expect("projection")
            .expect("conflict");
        assert_eq!(projection.candidate.novelty, KnowledgeNovelty::Conflicts);
        assert_eq!(projection.state, KnowledgeCandidateState::AwaitingApproval);
    }
}
